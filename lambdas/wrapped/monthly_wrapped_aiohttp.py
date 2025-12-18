import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

from lambdas.common.wrapped_helper import get_active_wrapped_users
from lambdas.common.spotify import Spotify
from lambdas.common.constants import USERS_TABLE_NAME, LOGO_BASE_64, BLACK_2025_BASE_64, WRAPPED_2026_LOGOS, LOGGER
from lambdas.common.dynamo_helpers import update_table_item, save_monthly_wrap

log = LOGGER.get_logger(__file__)


def get_last_month_key() -> str:
    """
    Get the month key for last month in YYYY-MM format.
    e.g., if today is Jan 15 2025, returns "2024-12"
    """
    today = datetime.now(timezone.utc)
    first_of_month = today.replace(day=1)
    last_month = first_of_month - timedelta(days=1)
    return last_month.strftime('%Y-%m')


async def aiohttp_wrapped_chron_job(event):
    """
    Main entry point for the monthly wrapped cron job.
    Processes all active users concurrently.
    """
    try:
        log.info("=" * 50)
        log.info("Starting AIOHTTP Wrapped Chron Job...")
        log.info("=" * 50)
        
        wrapped_users = get_active_wrapped_users()
        log.info(f"Found {len(wrapped_users)} active wrapped users")
        
        if not wrapped_users:
            log.info("No active users to process.")
            return []

        # Get the month key for this run (last month)
        month_key = get_last_month_key()
        log.info(f"Processing wrapped for month: {month_key}")

        # Use a single session for all users (connection pooling)
        connector = aiohttp.TCPConnector(limit=10)  # Limit concurrent connections
        timeout = aiohttp.ClientTimeout(total=300)  # 5 min timeout per user
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = [aiohttp_process_wrapped_user(user, session, month_key) for user in wrapped_users]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Separate successes from failures
        successes = []
        failures = []
        
        for user, result in zip(wrapped_users, results):
            if isinstance(result, Exception):
                log.error(f"❌ User {user['email']} failed: {result}")
                failures.append({"email": user['email'], "error": str(result)})
            else:
                log.info(f"✅ User {result} completed successfully")
                successes.append(result)

        log.info("=" * 50)
        log.info(f"Wrapped Cron Job Complete!")
        log.info(f"Successes: {len(successes)}, Failures: {len(failures)}")
        log.info("=" * 50)
        
        return successes
        
    except Exception as err:
        log.error(f"AIOHTTP Wrapped Chron Job: {err}")
        raise Exception("AIOHTTP Wrapped Chron Job failed") from err


async def aiohttp_process_wrapped_user(user: dict, session: aiohttp.ClientSession, month_key: str):
    """
    Process a single user's monthly wrapped data.
    Creates playlists and stores listening data to history table.
    """
    email = user.get('email', 'unknown')
    
    try:
        log.info(f"Processing user: {email}")
        
        # Initialize Spotify client with session
        spotify = Spotify(user, session)
        await spotify.aiohttp_initialize_wrapped()

        # Fetch top tracks and artists concurrently (all 6 terms at once)
        log.info(f"[{email}] Fetching top tracks and artists...")
        await asyncio.gather(
            spotify.top_tracks_short.aiohttp_set_top_tracks(),
            spotify.top_tracks_medium.aiohttp_set_top_tracks(),
            spotify.top_tracks_long.aiohttp_set_top_tracks(),
            spotify.top_artists_short.aiohttp_set_top_artists(),
            spotify.top_artists_medium.aiohttp_set_top_artists(),
            spotify.top_artists_long.aiohttp_set_top_artists()
        )

        # Build playlists based on the month
        log.info(f"[{email}] Building playlists (month: {spotify.last_month_number})...")
        wrapped_playlist_logo = WRAPPED_2026_LOGOS.get(spotify.last_month_number, LOGO_BASE_64) if spotify.this_year == "26" else LOGO_BASE_64
        playlist_tasks = [
            spotify.monthly_spotify_playlist.aiohttp_build_playlist(
                spotify.top_tracks_short.track_uri_list, 
                wrapped_playlist_logo
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

        # Get the listening data
        log.info(f"[{email}] Collecting listening data...")
        top_tracks = spotify.get_top_tracks_ids_last_month()
        top_artists = spotify.get_top_artists_ids_last_month()
        top_genres = spotify.get_top_genres_last_month()

        # Save to the NEW wrapped history table
        log.info(f"[{email}] Saving to wrapped history table for {month_key}...")
        save_monthly_wrap(
            email=email,
            month_key=month_key,
            top_song_ids=top_tracks,
            top_artist_ids=top_artists,
            top_genres=top_genres
        )

        # Update user table timestamp (keep for enrollment tracking)
        __update_user_timestamp(user)

        log.info(f"[{email}] ✅ User complete!")
        return email
        
    except Exception as err:
        log.error(f"[{email}] ❌ Failed: {err}")
        raise Exception(f"Process user {email} failed: {err}") from err


def __update_user_timestamp(user: dict):
    """
    Update user's timestamp in the main user table.
    No longer stores listening data here - just tracks last processed time.
    """
    try:
        user['updatedAt'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        update_table_item(USERS_TABLE_NAME, user)
    except Exception as err:
        log.error(f"Update User Timestamp: {err}")
        raise