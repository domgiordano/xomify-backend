"""
XOMIFY Weekly Release Radar Cron Job
====================================
Finalizes the previous week's release radar data for all enrolled users.

Schedule: Runs every Sunday ~2 AM Eastern
Week Definition: Sunday 00:00:00 to Saturday 23:59:59

Flow:
1. Get the PREVIOUS week key (the week that just ended Saturday)
2. For each user:
   a. ALWAYS fetch fresh from Spotify (to catch Fri/Sat releases)
   b. Mark as finalized=True
   c. Create/update Spotify playlist
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
    get_week_date_range
)

log = get_logger(__file__)


async def aiohttp_release_radar_chron_job(event) -> tuple[list, list]:
    """
    Main entry point for the weekly release radar cron job.
    
    Processes the PREVIOUS week (Sunday-Saturday that just ended).
    ALWAYS fetches fresh from Spotify to ensure complete data.
    
    Returns:
        Tuple of (successful_emails, failed_users)
    """
    log.info("=" * 50)
    log.info("📻 Starting Weekly Release Radar Cron Job")
    log.info("=" * 50)
    
    # Get PREVIOUS week key (the week that just ended Saturday)
    week_key = get_previous_week_key()
    start_date, end_date = get_week_date_range(week_key)
    
    log.info(f"Finalizing PREVIOUS week: {week_key}")
    log.info(f"Date range: {start_date.strftime('%Y-%m-%d')} (Sun) to {end_date.strftime('%Y-%m-%d')} (Sat)")
    
    # Get active users
    release_radar_users = get_active_release_radar_users()
    log.info(f"Found {len(release_radar_users)} active release radar users")
    
    if not release_radar_users:
        log.info("No active users to process")
        return [], []
    
    # Process users with connection pooling
    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=600)  # 10 min timeout
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            process_release_radar_user(user, session, week_key, start_date, end_date) 
            for user in release_radar_users
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Collect results
    successes = []
    failures = []
    
    for user, result in zip(release_radar_users, results):
        email = user.get('email', 'unknown')
        if isinstance(result, Exception):
            log.error(f"❌ {email}: {result}")
            failures.append({"email": email, "error": str(result)})
        else:
            track_count = result.get('tracks', 0)
            log.info(f"✅ {email}: {track_count} tracks finalized")
            successes.append(email)
    
    log.info("=" * 50)
    log.info(f"📻 Release Radar Cron Job Complete!")
    log.info(f"   ✅ Success: {len(successes)}")
    log.info(f"   ❌ Failed: {len(failures)}")
    log.info("=" * 50)
    
    return successes, failures


async def process_release_radar_user(
    user: dict, 
    session: aiohttp.ClientSession,
    week_key: str,
    start_date: datetime,
    end_date: datetime
) -> dict:
    """
    Finalize a single user's release radar for the previous week.
    
    ALWAYS fetches fresh from Spotify to catch any releases from late in the week
    that the user might have missed (e.g., Friday/Saturday releases if they 
    last visited on Thursday).
    
    Then marks the week as finalized=True.
    """
    email = user.get('email', 'unknown')
    
    try:
        log.info(f"[{email}] Processing week {week_key}...")
        
        # Initialize Spotify client
        spotify = Spotify(user, session)
        await spotify.aiohttp_initialize_release_radar()
        
        # ALWAYS fetch fresh from Spotify for complete week data
        log.info(f"[{email}] Fetching complete release data from Spotify...")
        
        # Get followed artists
        await spotify.followed_artists.aiohttp_get_followed_artists()
        artist_count = len(spotify.followed_artists.artist_id_list)
        log.info(f"[{email}] Found {artist_count} followed artists")
        
        # Set custom date window for the specific PREVIOUS week
        spotify.followed_artists.artist_tracks.week_start = start_date
        spotify.followed_artists.artist_tracks.week_end = end_date
        
        await spotify.followed_artists.aiohttp_get_followed_artist_latest_release()
        
        # Get release details
        releases = await get_release_details_for_history(
            spotify, 
            spotify.followed_artists.artist_tracks.album_uri_list,
            start_date,
            end_date
        )
        
        track_count = len(releases)
        log.info(f"[{email}] {track_count} releases for week {week_key}")
        
        # Get track URIs for playlist
        track_uris = spotify.followed_artists.artist_tracks.final_tracks_uris
        
        # Create or update playlist
        playlist_id = spotify.release_radar_playlist.id
        
        if track_uris:
            if not playlist_id:
                log.info(f"[{email}] Creating new Release Radar playlist...")
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
        
        # Save as FINALIZED
        log.info(f"[{email}] Saving finalized data...")
        save_release_radar_week(
            email=email,
            week_key=week_key,
            releases=releases,
            playlist_id=playlist_id,
            finalized=True  # THIS IS THE KEY - mark as finalized
        )
        
        log.info(f"[{email}] ✅ Week {week_key} finalized!")
        return {
            "email": email, 
            "tracks": track_count,
            "playlistId": playlist_id,
            "weekKey": week_key,
            "finalized": True
        }
        
    except Exception as err:
        log.error(f"[{email}] ❌ Failed: {err}")
        raise ReleaseRadarError(
            message=f"Process user {email} failed: {err}",
            function="process_release_radar_user"
        )


async def get_release_details_for_history(
    spotify, 
    album_uris: list,
    start_date: datetime,
    end_date: datetime
) -> list:
    """
    Get detailed release information for storing in history.
    Filters to only include releases within the specified week.
    
    Args:
        spotify: Spotify client instance
        album_uris: List of album URIs
        start_date: Start of week (Sunday)
        end_date: End of week (Saturday)
        
    Returns:
        List of release detail objects
    """
    try:
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
                    
                    # Parse and validate release date is within our week
                    release_date_str = album.get('release_date', '')
                    if not is_release_in_date_range(release_date_str, start_date, end_date):
                        continue
                    
                    # Format for storage (matches frontend interface)
                    releases.append({
                        'id': album.get('id'),
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
                log.warning(f"Failed to fetch album batch: {err}")
                continue
        
        return releases
        
    except Exception as err:
        log.error(f"Get release details failed: {err}")
        return []


def is_release_in_date_range(release_date_str: str, start_date: datetime, end_date: datetime) -> bool:
    """
    Check if a release date falls within the specified date range.
    
    Args:
        release_date_str: Release date string (YYYY-MM-DD, YYYY-MM, or YYYY)
        start_date: Start of range
        end_date: End of range
        
    Returns:
        True if release is within range
    """
    try:
        if not release_date_str or len(release_date_str) < 4:
            return False
        
        # Parse release date based on format
        if len(release_date_str) == 4:
            # Year only - can't determine, skip
            return False
        elif len(release_date_str) == 7:
            # Year-month only - treat as first of month
            release_date = datetime.strptime(release_date_str, "%Y-%m")
        else:
            # Full date
            release_date = datetime.strptime(release_date_str[:10], "%Y-%m-%d")
        
        # Compare dates only (not times)
        start_date_only = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_only = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        return start_date_only <= release_date <= end_date_only
        
    except Exception:
        return False


# Lambda handler
def handler(event, context):
    """AWS Lambda entry point for cron job."""
    try:
        successes, failures = asyncio.run(aiohttp_release_radar_chron_job(event))
        
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
