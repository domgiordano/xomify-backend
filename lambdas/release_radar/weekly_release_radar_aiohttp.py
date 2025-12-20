"""
XOMIFY Weekly Release Radar Cron Job
====================================
Fetches new releases from followed artists for all enrolled users.
"""

import asyncio
import aiohttp

from lambdas.common.logger import get_logger
from lambdas.common.errors import ReleaseRadarError, SpotifyAPIError
from lambdas.common.wrapped_helper import get_active_release_radar_users
from lambdas.common.spotify import Spotify
from lambdas.common.constants import BLACK_LOGO_BASE_64
from lambdas.common.dynamo_helpers import update_user_table_release_radar_id

log = get_logger(__file__)


async def aiohttp_release_radar_chron_job(event) -> tuple[list, list]:
    """
    Main entry point for the weekly release radar cron job.
    
    Returns:
        Tuple of (successful_emails, failed_users)
    """
    log.info("=" * 50)
    log.info("📻 Starting Weekly Release Radar Cron Job")
    log.info("=" * 50)
    
    # Get active users
    release_radar_users = get_active_release_radar_users()
    log.info(f"Found {len(release_radar_users)} active release radar users")
    
    if not release_radar_users:
        log.info("No active users to process")
        return [], []
    
    # Process users with connection pooling
    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=600)  # 10 min - release radar can be slow
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            process_release_radar_user(user, session) 
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
            artist_count = result.get('artists', 0)
            log.info(f"✅ {email}: {track_count} tracks from {artist_count} artists")
            successes.append(email)
    
    log.info("=" * 50)
    log.info(f"📻 Release Radar Cron Job Complete!")
    log.info(f"   ✅ Success: {len(successes)}")
    log.info(f"   ❌ Failed: {len(failures)}")
    log.info("=" * 50)
    
    return successes, failures


async def process_release_radar_user(user: dict, session: aiohttp.ClientSession) -> dict:
    """
    Process a single user's release radar.
    
    Args:
        user: User dict with email, refreshToken, etc.
        session: aiohttp session for API calls
        
    Returns:
        Dict with email, track count, artist count, and release window
        
    Raises:
        ReleaseRadarError: If processing fails
    """
    email = user.get('email', 'unknown')
    
    try:
        log.info(f"[{email}] Starting release radar processing...")
        
        # Initialize Spotify client
        spotify = Spotify(user, session)
        await spotify.aiohttp_initialize_release_radar()
        
        # Get followed artists
        log.info(f"[{email}] Fetching followed artists...")
        await spotify.followed_artists.aiohttp_get_followed_artists()
        artist_count = len(spotify.followed_artists.artist_id_list)
        log.info(f"[{email}] Found {artist_count} followed artists")
        
        # Log some sample artist IDs for debugging
        if artist_count > 0:
            sample_ids = spotify.followed_artists.artist_id_list[:5]
            log.debug(f"[{email}] Sample artist IDs: {sample_ids}")
        
        # Get new releases (this can take a while)
        log.info(f"[{email}] Scanning for new releases...")
        await spotify.followed_artists.aiohttp_get_followed_artist_latest_release()
        
        track_count = len(spotify.followed_artists.artist_tracks.final_tracks_uris)
        log.info(f"[{email}] Found {track_count} new tracks this week")
        
        # Create or update playlist
        if not spotify.release_radar_playlist.id:
            log.info(f"[{email}] Creating new Release Radar playlist...")
            await spotify.release_radar_playlist.aiohttp_build_playlist(
                spotify.followed_artists.artist_tracks.final_tracks_uris,
                BLACK_LOGO_BASE_64
            )
            # Save playlist ID to user record
            update_user_table_release_radar_id(user, spotify.release_radar_playlist.id)
            log.info(f"[{email}] Created playlist: {spotify.release_radar_playlist.id}")
        else:
            log.info(f"[{email}] Updating existing playlist: {spotify.release_radar_playlist.id}")
            await spotify.release_radar_playlist.aiohttp_update_playlist(
                spotify.followed_artists.artist_tracks.final_tracks_uris
            )
        
        log.info(f"[{email}] ✅ Release radar complete!")
        return {
            "email": email, 
            "tracks": track_count,
            "artists": artist_count
        }
        
    except Exception as err:
        log.error(f"[{email}] ❌ Failed: {err}")
        raise ReleaseRadarError(
            message=f"Process user {email} failed: {err}",
            function="process_release_radar_user"
        )
