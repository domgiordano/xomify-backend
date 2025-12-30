"""
XOMIFY Release Radar API Handler
================================
API endpoints for release radar history.

Endpoints:
- GET /release-radar/history - Get user's release radar history
- GET /release-radar/week/{weekKey} - Get specific week's data
- GET /release-radar/live - Fetch current week live from Spotify (daily refresh)
- GET /release-radar/check - Check if user has history
- POST /release-radar/backfill - Trigger backfill for user
"""

import json
import asyncio
import aiohttp

from lambdas.common.logger import get_logger
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

log = get_logger(__file__)

HANDLER = 'release-radar'


def handler(event, context):
    """
    Main API Gateway handler for release radar endpoints.
    """
    try:
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
        
        elif 'backfill' in path and http_method == 'POST':
            return trigger_backfill(body)
        
        elif 'check' in path and http_method == 'GET':
            return check_history(query_params.get('email'))
        
        else:
            return response(404, {'error': 'Not found'})
            
    except Exception as err:
        log.error(f"Release Radar API error: {err}")
        return response(500, {'error': str(err)})


# ============================================
# GET /release-radar/live
# ============================================

async def get_live_releases(params: dict) -> dict:
    """
    GET /release-radar/live
    
    Fetch current week's releases live from Spotify.
    Only fetches if not already updated today (daily refresh limit).
    
    Query params:
    - email: User's email (required)
    - force: If 'true', bypass daily refresh check
    
    Returns:
    - Current week data (either fresh from Spotify or cached from DB)
    - needsRefresh: Whether data was fetched fresh
    """
    email = params.get('email')
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    force_refresh = params.get('force', '').lower() == 'true'
    current_week_key = get_week_key()
    
    try:
        # Check if current week exists and is finalized
        existing_week = get_release_radar_week(email, current_week_key)
        
        if existing_week and existing_week.get('finalized'):
            # Week is finalized by cron, return DB data
            log.info(f"[{email}] Current week is finalized, returning DB data")
            return response(200, {
                'email': email,
                'weekKey': current_week_key,
                'week': existing_week,
                'source': 'database',
                'finalized': True,
                'needsRefresh': False
            })
        
        # Check if we need to refresh (not updated today)
        needs_refresh = force_refresh or check_week_needs_refresh(email, current_week_key)
        
        if not needs_refresh and existing_week:
            # Already updated today, return cached data
            log.info(f"[{email}] Already updated today, returning cached data")
            return response(200, {
                'email': email,
                'weekKey': current_week_key,
                'week': existing_week,
                'source': 'cache',
                'finalized': False,
                'needsRefresh': False
            })
        
        # Need to fetch live from Spotify
        log.info(f"[{email}] Fetching live releases from Spotify...")
        
        # Get user data for Spotify auth
        user = get_user_table_data(email)
        if not user:
            return response(404, {'error': 'User not found'})
        
        # Fetch releases
        releases = await fetch_current_week_releases(user)
        
        # Save to DB (non-finalized)
        existing_playlist_id = existing_week.get('playlistId') if existing_week else user.get('releaseRadarId')
        
        saved_week = save_release_radar_week(
            email=email,
            week_key=current_week_key,
            releases=releases,
            playlist_id=existing_playlist_id,
            finalized=False
        )
        
        log.info(f"[{email}] Saved {len(releases)} releases for current week")
        
        return response(200, {
            'email': email,
            'weekKey': current_week_key,
            'week': saved_week,
            'source': 'spotify',
            'finalized': False,
            'needsRefresh': True
        })
        
    except Exception as err:
        log.error(f"Get live releases error: {err}")
        return response(500, {'error': str(err)})


async def fetch_current_week_releases(user: dict) -> list:
    """
    Fetch current week's releases from Spotify for a user.
    
    Args:
        user: User dict with email, refreshToken, etc.
        
    Returns:
        List of release objects (normalized for storage)
    """
    email = user.get('email', 'unknown')
    
    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=120)  # 2 min timeout
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Initialize Spotify client
        spotify = Spotify(user, session)
        await spotify.aiohttp_initialize_release_radar()
        
        # Get followed artists
        log.info(f"[{email}] Fetching followed artists...")
        await spotify.followed_artists.aiohttp_get_followed_artists()
        artist_count = len(spotify.followed_artists.artist_id_list)
        log.info(f"[{email}] Found {artist_count} followed artists")
        
        # Get releases for current week (uses default dynamic calculation)
        log.info(f"[{email}] Scanning for releases...")
        await spotify.followed_artists.aiohttp_get_followed_artist_latest_release()
        
        # Get album details for the releases
        releases = await get_release_details(
            spotify,
            spotify.followed_artists.artist_tracks.album_uri_list
        )
        
        log.info(f"[{email}] Found {len(releases)} releases this week")
        return releases


async def get_release_details(spotify, album_uris: list) -> list:
    """
    Get detailed release information from album URIs.
    
    Args:
        spotify: Spotify client instance
        album_uris: List of album URIs
        
    Returns:
        List of normalized release objects
    """
    releases = []
    
    if not album_uris:
        return releases
    
    # Extract album IDs
    album_ids = [uri.split(':')[2] for uri in album_uris if uri]
    
    # Fetch album details in batches of 20
    for i in range(0, len(album_ids), 20):
        batch_ids = album_ids[i:i+20]
        ids_param = ','.join(batch_ids)
        url = f"https://api.spotify.com/v1/albums?ids={ids_param}"
        
        try:
            data = await fetch_json(
                spotify.aiohttp_session,
                url,
                headers=spotify.headers
            )
            
            for album in data.get('albums', []):
                if not album:
                    continue
                
                # Normalize for storage
                releases.append({
                    'id': album.get('id'),
                    'name': album.get('name'),
                    'artistName': album.get('artists', [{}])[0].get('name', 'Unknown'),
                    'artistId': album.get('artists', [{}])[0].get('id'),
                    'imageUrl': album.get('images', [{}])[0].get('url') if album.get('images') else None,
                    'albumType': album.get('album_type'),
                    'releaseDate': album.get('release_date'),
                    'totalTracks': album.get('total_tracks', 1),
                    'uri': album.get('uri')
                })
                
        except Exception as err:
            log.warning(f"Failed to fetch album batch: {err}")
            continue
    
    return releases


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
# POST /release-radar/backfill
# ============================================

def trigger_backfill(body: dict) -> dict:
    """
    POST /release-radar/backfill
    
    Trigger history backfill for a user (runs async in background).
    
    Body:
    - user: User object with email, refreshToken, etc.
    """
    from release_radar_backfill import invoke_backfill_async
    
    user = body.get('user')
    if not user:
        return response(400, {'error': 'Missing user data'})
    
    email = user.get('email')
    if not email:
        return response(400, {'error': 'Missing email in user data'})
    
    try:
        # Check if already has history (any, not just finalized)
        if check_user_has_history(email, finalized_only=False):
            return response(200, {
                'email': email,
                'status': 'skipped',
                'reason': 'history_exists'
            })
        
        # Invoke backfill Lambda asynchronously (returns immediately)
        result = invoke_backfill_async(user)
        
        return response(200, result)
        
    except Exception as err:
        log.error(f"Backfill trigger error: {err}")
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
