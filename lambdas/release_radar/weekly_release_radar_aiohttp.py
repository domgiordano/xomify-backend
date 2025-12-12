import asyncio
import aiohttp
from lambdas.common.wrapped_helper import get_active_release_radar_users
from lambdas.common.spotify import Spotify
from lambdas.common.constants import BLACK_LOGO_BASE_64, LOGGER
from lambdas.common.dynamo_helpers import update_user_table_release_radar_id

log = LOGGER.get_logger(__file__)

async def aiohttp_release_radar_chron_job(event):
    """
    Main entry point for the weekly release radar cron job.
    Fetches new releases from followed artists for all active users.
    """
    try:
        log.info("=" * 50)
        log.info("Starting AIOHTTP Release Radar Chron Job...")
        log.info("=" * 50)
        
        release_radar_users = get_active_release_radar_users()
        log.info(f"Found {len(release_radar_users)} active release radar users")
        
        if not release_radar_users:
            log.info("No active users to process.")
            return [], []

        # Use a single session with connection pooling
        connector = aiohttp.TCPConnector(limit=10)  # Limit concurrent connections
        timeout = aiohttp.ClientTimeout(total=600)  # 10 min timeout (release radar can be slow)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = [aiohttp_process_user(user, session) for user in release_radar_users]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Separate successes from failures
        successes = []
        failures = []
        
        for user, result in zip(release_radar_users, results):
            if isinstance(result, Exception):
                log.error(f"❌ User {user['email']} failed: {result}")
                failures.append({"email": user['email'], "error": str(result)})
            else:
                log.info(f"✅ User {result['email']} completed - {result['tracks']} tracks added")
                successes.append(result['email'])

        log.info("=" * 50)
        log.info(f"Release Radar Cron Job Complete!")
        log.info(f"Successes: {len(successes)}, Failures: {len(failures)}")
        log.info("=" * 50)
        
        return successes, failures
        
    except Exception as err:
        log.error(f"AIOHTTP Release Radar Chron Job: {err}")
        raise Exception(f"AIOHTTP Release Radar Chron Job: {err}") from err


async def aiohttp_process_user(user: dict, session: aiohttp.ClientSession):
    """
    Process a single user's release radar.
    Fetches new releases from all followed artists and updates their playlist.
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

        # Get new releases from followed artists
        log.info(f"[{email}] Scanning for new releases (this may take a while)...")
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
            log.info(f"[{email}] Created playlist ID: {spotify.release_radar_playlist.id}")
        else:
            log.info(f"[{email}] Updating existing playlist: {spotify.release_radar_playlist.id}")
            await spotify.release_radar_playlist.aiohttp_update_playlist(
                spotify.followed_artists.artist_tracks.final_tracks_uris
            )

        log.info(f"[{email}] ✅ Release radar complete!")
        return {"email": email, "tracks": track_count}
        
    except Exception as err:
        log.error(f"[{email}] ❌ Failed: {err}")
        raise Exception(f"Process user {email} failed: {err}") from err