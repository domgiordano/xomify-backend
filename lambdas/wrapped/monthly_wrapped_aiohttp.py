import asyncio
import aiohttp
from datetime import datetime, timezone

from lambdas.common.wrapped_helper import get_active_wrapped_users
from lambdas.common.spotify import Spotify
from lambdas.common.constants import WRAPPED_TABLE_NAME, LOGO_BASE_64, BLACK_2025_BASE_64, LOGGER
from lambdas.common.dynamo_helpers import update_table_item

log = LOGGER.get_logger(__file__)

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

        # Use a single session for all users (connection pooling)
        connector = aiohttp.TCPConnector(limit=10)  # Limit concurrent connections
        timeout = aiohttp.ClientTimeout(total=300)  # 5 min timeout per user
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = [aiohttp_process_wrapped_user(user, session) for user in wrapped_users]
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


async def aiohttp_process_wrapped_user(user: dict, session: aiohttp.ClientSession):
    """
    Process a single user's monthly wrapped data.
    Creates playlists and stores listening data.
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
        playlist_tasks = [
            spotify.monthly_spotify_playlist.aiohttp_build_playlist(
                spotify.top_tracks_short.track_uri_list, 
                LOGO_BASE_64
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

        # Store the data
        log.info(f"[{email}] Saving listening data to DynamoDB...")
        top_tracks_last_month = spotify.get_top_tracks_ids_last_month()
        top_artists_last_month = spotify.get_top_artists_ids_last_month()
        top_genres_last_month = spotify.get_top_genres_last_month()

        __update_user_table_entry(user, top_tracks_last_month, top_artists_last_month, top_genres_last_month)

        log.info(f"[{email}] ✅ User complete!")
        return email
        
    except Exception as err:
        log.error(f"[{email}] ❌ Failed: {err}")
        raise Exception(f"Process user {email} failed: {err}") from err


def __update_user_table_entry(user: dict, top_tracks: dict, top_artists: dict, top_genres: dict):
    """
    Update user's listening history in DynamoDB.
    Shifts last month's data to "two months ago" before storing new data.
    """
    try:
        # Shift tracks history
        user['topSongIdsTwoMonthsAgo'] = user.get('topSongIdsLastMonth', {})
        user['topSongIdsLastMonth'] = top_tracks
        
        # Shift artists history
        user['topArtistIdsTwoMonthsAgo'] = user.get('topArtistIdsLastMonth', {})
        user['topArtistIdsLastMonth'] = top_artists
        
        # Shift genres history
        user['topGenresTwoMonthsAgo'] = user.get('topGenresLastMonth', {})
        user['topGenresLastMonth'] = top_genres
        
        # Timestamp
        user['updatedAt'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        update_table_item(WRAPPED_TABLE_NAME, user)
        
    except Exception as err:
        log.error(f"Update User Table Entry: {err}")
        raise