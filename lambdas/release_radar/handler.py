"""
XOMIFY Release Radar API Handler
================================
API endpoints for release radar history AND weekly cron job.

Endpoints:
- GET /release-radar/history - Get user's release radar history
- GET /release-radar/week/{weekKey} - Get specific week's data
- GET /release-radar/live - Fetch current week live from Spotify (daily refresh)
- GET /release-radar/check - Check if user has history

Cron:
- Weekly cron job to finalize previous week's data and create playlists
"""

import json
import asyncio
import aiohttp
from datetime import datetime, timedelta

from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import is_cron_event, success_response
from lambdas.common.release_radar_dynamo import (
    get_user_release_radar_history,
    get_release_radar_week,
    get_release_radar_in_range,
    check_user_has_history,
    check_week_needs_refresh,
    get_week_key,
    get_current_week_date_range,
    save_release_radar_week
)
from lambdas.common.dynamo_helpers import get_user_table_data
from lambdas.common.spotify import Spotify
from lambdas.common.aiohttp_helper import fetch_json

# Import the cron job function
from weekly_release_radar_aiohttp import aiohttp_release_radar_chron_job

log = get_logger(__file__)

HANDLER = 'release-radar'


def handler(event, context):
    """
    Main Lambda handler for release radar.
    Handles both cron job invocation AND API requests.
    """
    try:
        # ========================================
        # CRON JOB - Weekly Release Radar
        # ========================================
        if is_cron_event(event):
            log.info("📻 Starting weekly release radar cron job...")
            successes, failures = asyncio.run(aiohttp_release_radar_chron_job(event))
            log.info(f"✅ Release radar cron complete - {len(successes)} users processed, {len(failures)} failed")
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
        path_params = event.get('pathParameters') or {}
        body = {}
        if event.get('body'):
            try:
                body = json.loads(event['body'])
            except:
                pass
        
        # Route request
        if 'live' in path and http_method == 'GET':
            return asyncio.run(get_live_releases(query_params))
        
        elif 'history' in path and http_method == 'GET':
            return get_history(query_params)
        
        elif 'week' in path and http_method == 'GET':
            week_key = path_params.get('weekKey') or query_params.get('weekKey')
            return get_week(query_params.get('email'), week_key)
        
        elif 'check' in path and http_method == 'GET':
            return check_history(query_params.get('email'))
        
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
    
    Smart fetch that combines current week + backfill when needed.
    
    Flow:
    1. FIRST: Check if current week was already updated today - if yes, return cached
    2. If not updated today AND user has < 30 weeks: do full 6-month fetch
    3. If not updated today AND user has >= 30 weeks: just fetch current week
    
    Query params:
    - email: User's email (required)
    - force: If 'true', bypass daily refresh check
    """
    email = params.get('email')
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    force_refresh = params.get('force', '').lower() == 'true'
    current_week_key = get_week_key()
    
    try:
        # Check existing history
        existing_weeks = get_user_release_radar_history(email, limit=30, finalized_only=False)
        existing_week_keys = {w.get('weekKey') for w in existing_weeks}
        has_enough_history = len(existing_weeks) >= 30
        
        # Check current week status
        current_week_data = next((w for w in existing_weeks if w.get('weekKey') == current_week_key), None)
        
        # PRIORITY 1: If current week is finalized, just return it
        if current_week_data and current_week_data.get('finalized'):
            log.info(f"[{email}] Current week is finalized, returning DB data")
            return response(200, {
                'email': email,
                'weekKey': current_week_key,
                'week': current_week_data,
                'source': 'database',
                'finalized': True
            })
        
        # PRIORITY 2: Check if current week was already updated TODAY
        # This takes precedence over backfill check - don't hit Spotify multiple times per day
        needs_refresh = force_refresh or check_week_needs_refresh(email, current_week_key)
        
        if not needs_refresh and current_week_data:
            log.info(f"[{email}] Current week already updated today, returning cached data")
            return response(200, {
                'email': email,
                'weekKey': current_week_key,
                'week': current_week_data,
                'source': 'cache',
                'finalized': False
            })
        
        # Need to fetch from Spotify - get user data
        user = get_user_table_data(email)
        if not user:
            return response(404, {'error': 'User not found'})
        
        # PRIORITY 3: If not enough history, do full 6-month fetch
        if not has_enough_history:
            log.info(f"[{email}] User has {len(existing_weeks)} weeks, doing full 6-month fetch...")
            current_week, weeks_saved = await fetch_and_save_all_releases(
                user, 
                existing_week_keys,
                current_week_key
            )
        else:
            # PRIORITY 4: Just fetch current week
            log.info(f"[{email}] User has enough history, fetching current week only...")
            current_week = await fetch_current_week_only(user, current_week_key)
            weeks_saved = 1 if current_week else 0
        
        if not current_week:
            # No releases this week, save empty week
            current_week = save_release_radar_week(
                email=email,
                week_key=current_week_key,
                releases=[],
                playlist_id=user.get('releaseRadarId'),
                finalized=False
            )
        
        log.info(f"[{email}] Returning current week with {len(current_week.get('releases', []))} releases")
        
        return response(200, {
            'email': email,
            'weekKey': current_week_key,
            'week': current_week,
            'source': 'spotify',
            'finalized': False,
            'weeksSaved': weeks_saved
        })
        
    except Exception as err:
        log.error(f"Get live releases error: {err}")
        return response(500, {'error': str(err)})


async def fetch_and_save_all_releases(
    user: dict, 
    existing_week_keys: set,
    current_week_key: str
) -> tuple:
    """
    Fetch 6 months of releases in ONE pass and save all weeks.
    Returns current week data immediately.
    
    Args:
        user: User dict
        existing_week_keys: Set of week keys that already exist (skip these)
        current_week_key: Current week key
        
    Returns:
        Tuple of (current_week_data, total_weeks_saved)
    """
    email = user.get('email', 'unknown')
    
    connector = aiohttp.TCPConnector(limit=5)
    timeout = aiohttp.ClientTimeout(total=300)  # 5 min for full fetch
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Initialize Spotify
        spotify = Spotify(user, session)
        await spotify.aiohttp_initialize_release_radar()
        
        # Get followed artists
        log.info(f"[{email}] Fetching followed artists...")
        await spotify.followed_artists.aiohttp_get_followed_artists()
        artist_ids = spotify.followed_artists.artist_id_list
        log.info(f"[{email}] Found {len(artist_ids)} followed artists")
        
        if not artist_ids:
            return None, 0
        
        # Fetch ALL releases from last 6 months
        cutoff_date = datetime.now() - timedelta(weeks=26)
        all_releases = []
        seen_ids = set()
        
        batch_size = 20
        total_artists = len(artist_ids)
        
        for i in range(0, total_artists, batch_size):
            batch = artist_ids[i:i+batch_size]
            
            for artist_id in batch:
                try:
                    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
                    url += "?include_groups=album,single,appears_on&limit=50"
                    
                    data = await fetch_json(session, url, headers=spotify.headers)
                    
                    for album in data.get('items', []):
                        album_id = album.get('id')
                        if album_id in seen_ids:
                            continue
                        
                        release_date_str = album.get('release_date', '')
                        release_date = parse_release_date(release_date_str)
                        if not release_date or release_date < cutoff_date:
                            continue
                        
                        seen_ids.add(album_id)
                        all_releases.append({
                            'id': album_id,
                            'name': album.get('name'),
                            'artistName': album.get('artists', [{}])[0].get('name', 'Unknown'),
                            'artistId': album.get('artists', [{}])[0].get('id'),
                            'imageUrl': album.get('images', [{}])[0].get('url') if album.get('images') else None,
                            'albumType': album.get('album_type'),
                            'releaseDate': release_date_str,
                            'totalTracks': album.get('total_tracks', 1),
                            'uri': album.get('uri'),
                            '_parsed_date': release_date
                        })
                except Exception as err:
                    log.debug(f"Failed to fetch releases for artist {artist_id}: {err}")
                    continue
            
            # Delay between batches
            if i + batch_size < total_artists:
                await asyncio.sleep(0.5)
        
        log.info(f"[{email}] Found {len(all_releases)} total releases in last 6 months")
        
        # Group by week
        releases_by_week = {}
        for release in all_releases:
            week_key = get_week_key(release['_parsed_date'])
            if week_key not in releases_by_week:
                releases_by_week[week_key] = []
            # Remove internal field before storing
            release_copy = {k: v for k, v in release.items() if not k.startswith('_')}
            releases_by_week[week_key].append(release_copy)
        
        log.info(f"[{email}] Grouped into {len(releases_by_week)} weeks")
        
        # Save all weeks
        current_week_data = None
        weeks_saved = 0
        playlist_id = user.get('releaseRadarId')
        
        for week_key, releases in releases_by_week.items():
            # Skip if already exists
            if week_key in existing_week_keys:
                log.debug(f"[{email}] Skipping existing week {week_key}")
                continue
            
            # Current week = not finalized, past weeks = finalized
            is_current = (week_key == current_week_key)
            
            try:
                saved = save_release_radar_week(
                    email=email,
                    week_key=week_key,
                    releases=releases,
                    playlist_id=playlist_id if is_current else None,
                    finalized=not is_current
                )
                weeks_saved += 1
                
                if is_current:
                    current_week_data = saved
                    log.info(f"[{email}] Saved current week {week_key} with {len(releases)} releases")
                else:
                    log.debug(f"[{email}] Saved historical week {week_key} with {len(releases)} releases")
                    
            except Exception as err:
                log.warning(f"[{email}] Failed to save week {week_key}: {err}")
        
        # If no releases this week, still create the current week entry
        if current_week_data is None:
            current_week_data = save_release_radar_week(
                email=email,
                week_key=current_week_key,
                releases=[],
                playlist_id=playlist_id,
                finalized=False
            )
            weeks_saved += 1
        
        log.info(f"[{email}] Saved {weeks_saved} weeks total")
        return current_week_data, weeks_saved


async def fetch_current_week_only(user: dict, current_week_key: str) -> dict:
    """
    Fetch just the current week's releases (for users with existing history).
    """
    email = user.get('email', 'unknown')
    
    connector = aiohttp.TCPConnector(limit=5)
    timeout = aiohttp.ClientTimeout(total=120)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        spotify = Spotify(user, session)
        await spotify.aiohttp_initialize_release_radar()
        
        # Get followed artists
        await spotify.followed_artists.aiohttp_get_followed_artists()
        artist_ids = spotify.followed_artists.artist_id_list
        
        if not artist_ids:
            return None
        
        # Get current week date range
        week_start, week_end = get_current_week_date_range()
        
        releases = []
        seen_ids = set()
        batch_size = 20
        
        for i in range(0, len(artist_ids), batch_size):
            batch = artist_ids[i:i+batch_size]
            
            for artist_id in batch:
                try:
                    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
                    url += "?include_groups=album,single,appears_on&limit=10"
                    
                    data = await fetch_json(session, url, headers=spotify.headers)
                    
                    for album in data.get('items', []):
                        album_id = album.get('id')
                        if album_id in seen_ids:
                            continue
                        
                        release_date_str = album.get('release_date', '')
                        release_date = parse_release_date(release_date_str)
                        
                        if not release_date:
                            continue
                        
                        # Check if in current week
                        if not (week_start.date() <= release_date.date() <= week_end.date()):
                            continue
                        
                        seen_ids.add(album_id)
                        releases.append({
                            'id': album_id,
                            'name': album.get('name'),
                            'artistName': album.get('artists', [{}])[0].get('name', 'Unknown'),
                            'artistId': album.get('artists', [{}])[0].get('id'),
                            'imageUrl': album.get('images', [{}])[0].get('url') if album.get('images') else None,
                            'albumType': album.get('album_type'),
                            'releaseDate': release_date_str,
                            'totalTracks': album.get('total_tracks', 1),
                            'uri': album.get('uri')
                        })
                except Exception as err:
                    log.debug(f"Failed to fetch releases for artist {artist_id}: {err}")
                    continue
            
            if i + batch_size < len(artist_ids):
                await asyncio.sleep(0.3)
        
        # Save current week
        playlist_id = user.get('releaseRadarId')
        saved = save_release_radar_week(
            email=email,
            week_key=current_week_key,
            releases=releases,
            playlist_id=playlist_id,
            finalized=False
        )
        
        log.info(f"[{email}] Saved current week with {len(releases)} releases")
        return saved


def parse_release_date(date_str: str) -> datetime:
    """Parse release date string to datetime."""
    if not date_str or len(date_str) < 4:
        return None
    try:
        if len(date_str) == 4:
            return datetime(int(date_str), 1, 1)
        elif len(date_str) == 7:
            return datetime.strptime(date_str, '%Y-%m')
        else:
            return datetime.strptime(date_str[:10], '%Y-%m-%d')
    except:
        return None


# ============================================
# GET /release-radar/history
# ============================================

def get_history(params: dict) -> dict:
    """
    GET /release-radar/history
    
    Query params:
    - email: User's email (required)
    - limit: Max results (optional, default 26 = ~6 months)
    - startWeek: Start of range (optional)
    - endWeek: End of range (optional)
    - finalizedOnly: If 'true', only return finalized weeks (default: false)
    """
    email = params.get('email')
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    try:
        # Check for range query
        start_week = params.get('startWeek')
        end_week = params.get('endWeek')
        # Default to including ALL weeks (finalized and non-finalized)
        finalized_only = params.get('finalizedOnly', '').lower() == 'true'
        
        if start_week and end_week:
            weeks = get_release_radar_in_range(email, start_week, end_week, finalized_only=finalized_only)
        else:
            limit = int(params.get('limit', 26))
            weeks = get_user_release_radar_history(email, limit=limit, finalized_only=finalized_only)
        
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
# GET /release-radar/week/{weekKey}
# ============================================

def get_week(email: str, week_key: str) -> dict:
    """
    GET /release-radar/week/{weekKey}
    
    Query params:
    - email: User's email (required)
    """
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    if not week_key:
        return response(400, {'error': 'Missing weekKey parameter'})
    
    try:
        week_data = get_release_radar_week(email, week_key)
        
        if not week_data:
            return response(404, {
                'error': 'Week not found',
                'email': email,
                'weekKey': week_key
            })
        
        return response(200, week_data)
        
    except Exception as err:
        log.error(f"Get week error: {err}")
        return response(500, {'error': str(err)})


# ============================================
# GET /release-radar/check
# ============================================

def check_history(email: str) -> dict:
    """
    GET /release-radar/check
    
    Check if user has any history (finalized OR non-finalized).
    
    Query params:
    - email: User's email (required)
    """
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    try:
        # Check for ANY history (not just finalized)
        has_history = check_user_has_history(email, finalized_only=False)
        current_week = get_week_key()
        
        # Also check if current week needs refresh
        needs_refresh = check_week_needs_refresh(email, current_week)
        
        return response(200, {
            'email': email,
            'hasHistory': has_history,
            'currentWeek': current_week,
            'currentWeekNeedsRefresh': needs_refresh
        })
        
    except Exception as err:
        log.error(f"Check history error: {err}")
        return response(500, {'error': str(err)})


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