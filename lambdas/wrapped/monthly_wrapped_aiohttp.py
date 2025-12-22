"""
XOMIFY Monthly Wrapped Cron Job
===============================
Processes all active users monthly to generate wrapped playlists.
"""

import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

from lambdas.common.logger import get_logger
from lambdas.common.errors import WrappedError, SpotifyAPIError
from lambdas.common.wrapped_helper import get_active_wrapped_users
from lambdas.common.spotify import Spotify
from lambdas.common.constants import USERS_TABLE_NAME, LOGO_BASE_64, BLACK_2025_BASE_64, WRAPPED_2026_LOGOS
from lambdas.common.dynamo_helpers import update_table_item, save_monthly_wrap

log = get_logger(__file__)


def get_last_month_key() -> str:
    """
    Get the month key for last month in YYYY-MM format.
    e.g., if today is Jan 15 2025, returns "2024-12"
    """
    today = datetime.now(timezone.utc)
    first_of_month = today.replace(day=1)
    last_month = first_of_month - timedelta(days=1)
    return last_month.strftime('%Y-%m')


async def aiohttp_wrapped_chron_job(event) -> list:
    """
    Main entry point for the monthly wrapped cron job.
    Processes all active users concurrently.
    
    Returns:
        List of successfully processed user emails
    """
    log.info("=" * 50)
    log.info("🎵 Starting Monthly Wrapped Cron Job")
    log.info("=" * 50)
    
    # Get active users
    wrapped_users = get_active_wrapped_users()
    log.info(f"Found {len(wrapped_users)} active wrapped users")
    
    if not wrapped_users:
        log.info("No active users to process")
        return []
    
    # Get the month key for this run
    month_key = get_last_month_key()
    log.info(f"Processing wrapped for month: {month_key}")
    
    # Process users with connection pooling
    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=300)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            process_wrapped_user(user, session, month_key) 
            for user in wrapped_users
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Collect results
    successes = []
    failures = []
    
    for user, result in zip(wrapped_users, results):
        email = user.get('email', 'unknown')
        if isinstance(result, Exception):
            log.error(f"❌ {email}: {result}")
            failures.append({"email": email, "error": str(result)})
        else:
            log.info(f"✅ {email}: Complete")
            successes.append(result)
    
    log.info("=" * 50)
    log.info(f"🎵 Wrapped Cron Job Complete!")
    log.info(f"   ✅ Success: {len(successes)}")
    log.info(f"   ❌ Failed: {len(failures)}")
    log.info("=" * 50)
    
    return successes


async def process_wrapped_user(user: dict, session: aiohttp.ClientSession, month_key: str) -> str:
    """
    Process a single user's monthly wrapped data.
    Creates playlists and stores listening data to history table.
    
    Args:
        user: User dict with email, refreshToken, etc.
        session: aiohttp session for API calls
        month_key: Month to process (YYYY-MM)
        
    Returns:
        User's email on success
        
    Raises:
        WrappedError: If processing fails
    """
    email = user.get('email', 'unknown')
    
    try:
        log.info(f"[{email}] Starting wrapped processing...")
        
        # Initialize Spotify client
        spotify = Spotify(user, session)
        await spotify.aiohttp_initialize_wrapped()
        
        # Fetch all data concurrently
        log.info(f"[{email}] Fetching top tracks and artists...")
        await asyncio.gather(
            spotify.top_tracks_short.aiohttp_set_top_tracks(),
            spotify.top_tracks_medium.aiohttp_set_top_tracks(),
            spotify.top_tracks_long.aiohttp_set_top_tracks(),
            spotify.top_artists_short.aiohttp_set_top_artists(),
            spotify.top_artists_medium.aiohttp_set_top_artists(),
            spotify.top_artists_long.aiohttp_set_top_artists()
        )
        
        # Build playlists
        log.info(f"[{email}] Building playlists (month: {spotify.last_month_number})...")
        
        # Select logo based on year/month
        if spotify.this_year == "26":
            playlist_logo = WRAPPED_2026_LOGOS.get(spotify.last_month_number, LOGO_BASE_64)
        else:
            playlist_logo = LOGO_BASE_64
        
        playlist_tasks = [
            spotify.monthly_spotify_playlist.aiohttp_build_playlist(
                spotify.top_tracks_short.track_uri_list,
                playlist_logo
            )
        ]
        
        # June = first half of year playlist
        if spotify.last_month_number == 6:
            log.info(f"[{email}] Adding first half of year playlist")
            playlist_tasks.append(
                spotify.first_half_of_year_spotify_playlist.aiohttp_build_playlist(
                    spotify.top_tracks_medium.track_uri_list,
                    LOGO_BASE_64
                )
            )
        
        # December = full year playlist
        if spotify.last_month_number == 12:
            log.info(f"[{email}] Adding full year wrapped playlist")
            playlist_tasks.append(
                spotify.full_year_spotify_playlist.aiohttp_build_playlist(
                    spotify.top_tracks_long.track_uri_list,
                    BLACK_2025_BASE_64
                )
            )
        
        await asyncio.gather(*playlist_tasks)
        
        # Collect listening data
        log.info(f"[{email}] Saving listening data...")
        top_tracks = spotify.get_top_tracks_ids_last_month()
        top_artists = spotify.get_top_artists_ids_last_month()
        top_genres = spotify.get_top_genres_last_month()
        
        # Save to history table
        save_monthly_wrap(
            email=email,
            month_key=month_key,
            top_song_ids=top_tracks,
            top_artist_ids=top_artists,
            top_genres=top_genres,
            playlist_id=spotify.monthly_spotify_playlist.id
        )
        
        # Update user timestamp
        _update_user_timestamp(user)
        
        log.info(f"[{email}] ✅ Wrapped complete!")
        return email
        
    except Exception as err:
        log.error(f"[{email}] ❌ Failed: {err}")
        raise WrappedError(
            message=f"Process user {email} failed: {err}",
            function="process_wrapped_user"
        )


def _update_user_timestamp(user: dict):
    """Update user's last processed timestamp."""
    try:
        user['updatedAt'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        update_table_item(USERS_TABLE_NAME, user)
    except Exception as err:
        log.warning(f"Failed to update user timestamp: {err}")
