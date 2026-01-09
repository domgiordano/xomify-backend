"""
XOMIFY Weekly Release Radar Cron Job
====================================
Processes release radar for all enrolled users.

Schedule: Runs every Saturday morning (~2 AM Eastern)
Week Definition: Saturday 00:00:00 to Friday 23:59:59

Flow:
1. Get the PREVIOUS week (last Saturday through yesterday Friday)
2. For each enrolled user:
   a. Get all followed artists
   b. Fetch releases from that week
   c. Save to DynamoDB history
   d. Create/update Spotify playlist
3. Email sender runs 15 min later
"""

import asyncio
import aiohttp
from datetime import datetime

from lambdas.common.logger import get_logger
from lambdas.common.errors import ReleaseRadarError
from lambdas.common.wrapped_helper import get_active_release_radar_users
from lambdas.common.spotify import Spotify
from lambdas.common.constants import BLACK_LOGO_BASE_64
from lambdas.common.dynamo_helpers import update_user_table_release_radar_id
from lambdas.common.aiohttp_helper import fetch_json
from lambdas.common.release_radar_dynamo import (
    save_release_radar_week,
    get_previous_week_key,
    get_week_date_range,
    format_week_display
)

log = get_logger(__file__)


async def release_radar_cron_job(event) -> tuple[list, list]:
    """
    Main entry point for the weekly release radar cron job.
    
    Processes the PREVIOUS week (Saturday-Friday that just ended).
    
    Returns:
        Tuple of (successful_emails, failed_users)
    """
    log.info("=" * 60)
    log.info("📻 RELEASE RADAR CRON JOB STARTING")
    log.info("=" * 60)
    
    # Get PREVIOUS week key (the week that ended yesterday Friday)
    week_key = get_previous_week_key()
    start_date, end_date = get_week_date_range(week_key)
    
    log.info(f"Processing week: {week_key}")
    log.info(f"Date range: {start_date.strftime('%Y-%m-%d')} (Sat) to {end_date.strftime('%Y-%m-%d')} (Fri)")
    log.info(f"Display: {format_week_display(week_key)}")
    
    # Get active users
    users = get_active_release_radar_users()
    log.info(f"Found {len(users)} enrolled users")
    
    if not users:
        log.info("No active users to process")
        return [], []
    
    # Process users with connection pooling
    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=600)  # 10 min timeout
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            process_user(user, session, week_key, start_date, end_date) 
            for user in users
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Collect results
    successes = []
    failures = []
    
    for user, result in zip(users, results):
        email = user.get('email', 'unknown')
        if isinstance(result, Exception):
            log.error(f"❌ {email}: {result}")
            failures.append({"email": email, "error": str(result)})
        else:
            release_count = result.get('releaseCount', 0)
            log.info(f"✅ {email}: {release_count} releases")
            successes.append(email)
    
    log.info("=" * 60)
    log.info(f"📻 RELEASE RADAR CRON JOB COMPLETE")
    log.info(f"   ✅ Success: {len(successes)}")
    log.info(f"   ❌ Failed: {len(failures)}")
    log.info("=" * 60)
    
    return successes, failures


async def process_user(
    user: dict, 
    session: aiohttp.ClientSession,
    week_key: str,
    start_date: datetime,
    end_date: datetime
) -> dict:
    """
    Process a single user's release radar for the week.
    
    1. Get followed artists
    2. Fetch all releases from the week
    3. Save to history
    4. Create/update playlist
    """
    email = user.get('email', 'unknown')
    
    try:
        log.info(f"[{email}] Processing...")
        
        # Initialize Spotify client
        spotify = Spotify(user, session)
        await spotify.aiohttp_initialize_release_radar()
        
        # Get followed artists
        log.info(f"[{email}] Fetching followed artists...")
        await spotify.followed_artists.aiohttp_get_followed_artists()
        artist_ids = spotify.followed_artists.artist_id_list
        log.info(f"[{email}] Found {len(artist_ids)} followed artists")
        
        if not artist_ids:
            log.info(f"[{email}] No followed artists, skipping")
            return {"email": email, "releaseCount": 0, "skipped": True}
        
        # Fetch releases for this specific week
        log.info(f"[{email}] Fetching releases for {week_key}...")
        releases = await fetch_releases_for_week(
            spotify,
            artist_ids,
            start_date,
            end_date
        )
        log.info(f"[{email}] Found {len(releases)} releases")
        
        # Get track URIs for playlist
        track_uris = []
        for release in releases:
            uri = release.get('uri')
            if uri:
                # If it's an album, we need to get its tracks
                if 'album' in uri:
                    album_tracks = await get_album_track_uris(spotify, uri)
                    track_uris.extend(album_tracks)
                else:
                    track_uris.append(uri)
        
        # Remove duplicates
        track_uris = list(set(track_uris))
        log.info(f"[{email}] Total tracks for playlist: {len(track_uris)}")
        
        # Create or update playlist
        playlist_id = spotify.release_radar_playlist.id
        
        if track_uris:
            if not playlist_id:
                log.info(f"[{email}] Creating new playlist...")
                await spotify.release_radar_playlist.aiohttp_build_playlist(
                    track_uris,
                    BLACK_LOGO_BASE_64
                )
                playlist_id = spotify.release_radar_playlist.id
                update_user_table_release_radar_id(user, playlist_id)
                log.info(f"[{email}] Created playlist: {playlist_id}")
            else:
                log.info(f"[{email}] Updating playlist: {playlist_id}")
                await spotify.release_radar_playlist.aiohttp_update_playlist(track_uris)
        else:
            log.info(f"[{email}] No tracks to add to playlist")
        
        # Save to history
        saved = save_release_radar_week(
            email=email,
            week_key=week_key,
            releases=releases,
            playlist_id=playlist_id
        )
        
        log.info(f"[{email}] ✅ Complete!")
        
        return {
            "email": email,
            "releaseCount": len(releases),
            "trackCount": len(track_uris),
            "playlistId": playlist_id,
            "weekKey": week_key
        }
        
    except Exception as err:
        log.error(f"[{email}] ❌ Failed: {err}")
        raise ReleaseRadarError(
            message=f"Process user {email} failed: {err}",
            function="process_user"
        )


async def fetch_releases_for_week(
    spotify,
    artist_ids: list,
    start_date: datetime,
    end_date: datetime
) -> list:
    """
    Fetch all releases from followed artists within the week.
    
    Args:
        spotify: Spotify client
        artist_ids: List of artist IDs to check
        start_date: Saturday start of week
        end_date: Friday end of week
        
    Returns:
        List of normalized release objects
    """
    releases = []
    seen_ids = set()
    
    # Process in batches
    batch_size = 20
    total = len(artist_ids)
    
    for i in range(0, total, batch_size):
        batch = artist_ids[i:i+batch_size]
        
        for artist_id in batch:
            try:
                # Get albums, singles, and appears_on
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
                        
                        # Check if release is in our week
                        release_date_str = album.get('release_date', '')
                        if not is_in_week(release_date_str, start_date, end_date):
                            continue
                        
                        seen_ids.add(album_id)
                        
                        # Normalize for storage
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
                        
                        log.debug(f"Found: {album.get('name')} by {album.get('artists', [{}])[0].get('name')} ({release_date_str})")
                        
            except Exception as err:
                log.debug(f"Failed to fetch releases for artist {artist_id}: {err}")
                continue
        
        # Small delay between batches
        if i + batch_size < total:
            await asyncio.sleep(0.3)
    
    # Sort by release date (newest first)
    releases.sort(key=lambda x: x.get('releaseDate', ''), reverse=True)
    
    return releases


def is_in_week(release_date_str: str, start_date: datetime, end_date: datetime) -> bool:
    """
    Check if a release date falls within the week.
    
    Args:
        release_date_str: Date string (YYYY-MM-DD, YYYY-MM, or YYYY)
        start_date: Saturday start
        end_date: Friday end
        
    Returns:
        True if release is in the week
    """
    if not release_date_str or len(release_date_str) < 4:
        return False
    
    try:
        # Parse based on format
        if len(release_date_str) == 4:
            # Year only - can't determine week
            return False
        elif len(release_date_str) == 7:
            # Year-month - treat as first of month
            release_date = datetime.strptime(release_date_str, '%Y-%m')
        else:
            # Full date
            release_date = datetime.strptime(release_date_str[:10], '%Y-%m-%d')
        
        # Compare dates
        start = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        return start <= release_date <= end
        
    except Exception:
        return False


async def get_album_track_uris(spotify, album_uri: str) -> list:
    """Get all track URIs from an album."""
    try:
        album_id = album_uri.split(':')[2]
        url = f"https://api.spotify.com/v1/albums/{album_id}/tracks?limit=50"
        
        data = await fetch_json(
            spotify.aiohttp_session,
            url,
            headers=spotify.headers
        )
        
        return [track.get('uri') for track in data.get('items', []) if track.get('uri')]
        
    except Exception as err:
        log.debug(f"Failed to get album tracks for {album_uri}: {err}")
        return []


# Lambda handler
def handler(event, context):
    """AWS Lambda entry point."""
    try:
        successes, failures = asyncio.run(release_radar_cron_job(event))
        
        return {
            'statusCode': 200,
            'body': {
                'successfulUsers': successes,
                'failedUsers': failures
            }
        }
    except Exception as err:
        log.error(f"Lambda handler error: {err}")
        return {
            'statusCode': 500,
            'body': {'error': str(err)}
        }
