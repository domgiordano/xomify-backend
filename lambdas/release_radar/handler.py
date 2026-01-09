"""
XOMIFY Release Radar API Handler
================================
API endpoints for release radar.

Endpoints:
- GET /release-radar/history - Get user's release radar history
- GET /release-radar/live - Get current week's releases (live from Spotify)
- GET /release-radar/check - Check enrollment status
- POST /release-radar/refresh - Force refresh current week (admin/debug)

Cron:
- Weekly cron job runs Saturday morning via CloudWatch Events
"""

import json
import asyncio
import aiohttp
from datetime import datetime

from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import is_cron_event, success_response
from lambdas.common.dynamo_helpers import get_user_table_data
from lambdas.common.spotify import Spotify
from lambdas.common.aiohttp_helper import fetch_json
from lambdas.common.release_radar_dynamo import (
    get_user_release_radar_history,
    get_release_radar_week,
    check_user_has_history,
    get_week_key,
    get_week_date_range,
    format_week_display
)

# Import cron job
from weekly_release_radar_aiohttp import release_radar_cron_job

log = get_logger(__file__)


def handler(event, context):
    """
    Main Lambda handler for release radar.
    Routes to cron job or API endpoints.
    """
    try:
        # ========================================
        # CRON JOB - Weekly Release Radar
        # ========================================
        if is_cron_event(event):
            log.info("📻 Starting weekly release radar cron job...")
            successes, failures = asyncio.run(release_radar_cron_job(event))
            return success_response({
                "successfulUsers": successes,
                "failedUsers": failures
            }, is_api=False)
        
        # ========================================
        # API REQUESTS
        # ========================================
        http_method = event.get('httpMethod', event.get('requestContext', {}).get('http', {}).get('method'))
        path = event.get('path', event.get('rawPath', ''))
        
        log.info(f"Release Radar API: {http_method} {path}")
        
        # Parse request
        query_params = event.get('queryStringParameters') or {}
        
        # Route request
        if 'live' in path and http_method == 'GET':
            return asyncio.run(get_live_releases(query_params))
        
        elif 'history' in path and http_method == 'GET':
            return get_history(query_params)
        
        elif 'check' in path and http_method == 'GET':
            return check_status(query_params)
        
        elif 'refresh' in path and http_method == 'POST':
            return asyncio.run(refresh_current_week(query_params))
        
        else:
            return response(404, {'error': 'Not found'})
            
    except Exception as err:
        log.error(f"Release Radar handler error: {err}")
        return response(500, {'error': str(err)})


# ============================================
# GET /release-radar/live
# ============================================

async def get_live_releases(params: dict) -> dict:
    """
    GET /release-radar/live
    
    Fetch current week's releases live from Spotify.
    Does NOT save to database - that's the cron's job.
    
    Query params:
    - email: User's email (required)
    """
    email = params.get('email')
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    try:
        # Get user
        user = get_user_table_data(email)
        if not user:
            return response(404, {'error': 'User not found'})
        
        # Get current week info
        current_week_key = get_week_key()
        start_date, end_date = get_week_date_range(current_week_key)
        
        log.info(f"[{email}] Fetching live releases for week {current_week_key}")
        log.info(f"[{email}] Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Fetch from Spotify
        connector = aiohttp.TCPConnector(limit=5)
        timeout = aiohttp.ClientTimeout(total=120)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            spotify = Spotify(user, session)
            await spotify.aiohttp_initialize_release_radar()
            
            # Get followed artists
            await spotify.followed_artists.aiohttp_get_followed_artists()
            artist_ids = spotify.followed_artists.artist_id_list
            
            log.info(f"[{email}] Found {len(artist_ids)} followed artists")
            
            if not artist_ids:
                return response(200, {
                    'email': email,
                    'weekKey': current_week_key,
                    'weekDisplay': format_week_display(current_week_key),
                    'startDate': start_date.strftime('%Y-%m-%d'),
                    'endDate': end_date.strftime('%Y-%m-%d'),
                    'releases': [],
                    'artistCount': 0,
                    'releaseCount': 0,
                    'trackCount': 0
                })
            
            # Fetch releases
            releases = await fetch_week_releases(
                spotify,
                artist_ids,
                start_date,
                end_date
            )
        
        # Calculate stats
        unique_artists = set(r.get('artistId') for r in releases if r.get('artistId'))
        total_tracks = sum(r.get('totalTracks', 1) for r in releases)
        album_count = len([r for r in releases if r.get('albumType') == 'album'])
        single_count = len([r for r in releases if r.get('albumType') == 'single'])
        
        log.info(f"[{email}] Found {len(releases)} releases from {len(unique_artists)} artists")
        
        return response(200, {
            'email': email,
            'weekKey': current_week_key,
            'weekDisplay': format_week_display(current_week_key),
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'releases': releases,
            'artistCount': len(unique_artists),
            'releaseCount': len(releases),
            'trackCount': total_tracks,
            'albumCount': album_count,
            'singleCount': single_count
        })
        
    except Exception as err:
        log.error(f"Get live releases error: {err}")
        return response(500, {'error': str(err)})


async def fetch_week_releases(
    spotify,
    artist_ids: list,
    start_date: datetime,
    end_date: datetime
) -> list:
    """Fetch releases for the specified week."""
    releases = []
    seen_ids = set()
    
    batch_size = 20
    
    for i in range(0, len(artist_ids), batch_size):
        batch = artist_ids[i:i+batch_size]
        
        for artist_id in batch:
            try:
                for include_group in ['album', 'single', 'appears_on']:
                    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
                    url += f"?include_groups={include_group}&limit=10"
                    
                    data = await fetch_json(
                        spotify.aiohttp_session,
                        url,
                        headers=spotify.headers
                    )
                    
                    for album in data.get('items', []):
                        album_id = album.get('id')
                        if not album_id or album_id in seen_ids:
                            continue
                        
                        release_date_str = album.get('release_date', '')
                        if not is_in_date_range(release_date_str, start_date, end_date):
                            continue
                        
                        seen_ids.add(album_id)
                        
                        releases.append({
                            'albumId': album_id,
                            'albumName': album.get('name'),
                            'albumType': album.get('album_type'),
                            'artistId': album.get('artists', [{}])[0].get('id'),
                            'artistName': album.get('artists', [{}])[0].get('name', 'Unknown'),
                            'releaseDate': release_date_str,
                            'totalTracks': album.get('total_tracks', 1),
                            'imageUrl': album.get('images', [{}])[0].get('url') if album.get('images') else None,
                            'spotifyUrl': album.get('external_urls', {}).get('spotify'),
                            'uri': album.get('uri')
                        })
                        
            except Exception as err:
                log.debug(f"Failed for artist {artist_id}: {err}")
                continue
        
        if i + batch_size < len(artist_ids):
            await asyncio.sleep(0.2)
    
    # Sort newest first
    releases.sort(key=lambda x: x.get('releaseDate', ''), reverse=True)
    return releases


def is_in_date_range(release_date_str: str, start_date: datetime, end_date: datetime) -> bool:
    """Check if release date is in range."""
    if not release_date_str or len(release_date_str) < 7:
        return False
    
    try:
        if len(release_date_str) == 7:
            release_date = datetime.strptime(release_date_str, '%Y-%m')
        else:
            release_date = datetime.strptime(release_date_str[:10], '%Y-%m-%d')
        
        return start_date <= release_date <= end_date
    except:
        return False


# ============================================
# GET /release-radar/history
# ============================================

def get_history(params: dict) -> dict:
    """
    GET /release-radar/history
    
    Get user's release radar history from database.
    
    Query params:
    - email: User's email (required)
    - limit: Max results (optional, default 26)
    """
    email = params.get('email')
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    try:
        limit = int(params.get('limit', 26))
        weeks = get_user_release_radar_history(email, limit=limit)
        
        # Add display name to each week
        for week in weeks:
            week['weekDisplay'] = format_week_display(week.get('weekKey', ''))
        
        return response(200, {
            'email': email,
            'weeks': weeks,
            'count': len(weeks),
            'currentWeek': get_week_key()
        })
        
    except Exception as err:
        log.error(f"Get history error: {err}")
        return response(500, {'error': str(err)})


# ============================================
# GET /release-radar/check
# ============================================

def check_status(params: dict) -> dict:
    """
    GET /release-radar/check
    
    Check user's release radar status.
    
    Query params:
    - email: User's email (required)
    """
    email = params.get('email')
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    try:
        has_history = check_user_has_history(email)
        current_week = get_week_key()
        start_date, end_date = get_week_date_range(current_week)
        
        # Check if user is enrolled
        user = get_user_table_data(email)
        is_enrolled = user.get('activeReleaseRadar', False) if user else False
        
        return response(200, {
            'email': email,
            'enrolled': is_enrolled,
            'hasHistory': has_history,
            'currentWeek': current_week,
            'currentWeekDisplay': format_week_display(current_week),
            'weekStartDate': start_date.strftime('%Y-%m-%d'),
            'weekEndDate': end_date.strftime('%Y-%m-%d')
        })
        
    except Exception as err:
        log.error(f"Check status error: {err}")
        return response(500, {'error': str(err)})


# ============================================
# POST /release-radar/refresh (Admin/Debug)
# ============================================

async def refresh_current_week(params: dict) -> dict:
    """
    POST /release-radar/refresh
    
    Force refresh and save current week's releases.
    This is mainly for testing/debugging.
    
    Query params:
    - email: User's email (required)
    """
    email = params.get('email')
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    # For now, just call get_live and return success
    # In production, you might want to save this
    result = await get_live_releases(params)
    return result


# ============================================
# Response Helper
# ============================================

def response(status_code: int, body: dict) -> dict:
    """Build API Gateway response."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
        },
        'body': json.dumps(body, default=str)
    }
