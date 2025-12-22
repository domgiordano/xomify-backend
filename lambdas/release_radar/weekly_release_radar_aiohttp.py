"""
XOMIFY Weekly Release Radar Cron Job
====================================
Fetches new releases from followed artists for all enrolled users,
saves history to DynamoDB, and creates/updates playlists.

Runs every Friday at 8:00 AM Eastern.
"""

import asyncio
import aiohttp

from lambdas.common.logger import get_logger
from lambdas.common.errors import ReleaseRadarError, SpotifyAPIError
from lambdas.common.wrapped_helper import get_active_release_radar_users
from lambdas.common.spotify import Spotify
from lambdas.common.constants import BLACK_LOGO_BASE_64
from lambdas.common.dynamo_helpers import update_user_table_release_radar_id
from lambdas.common.release_radar_dynamo import (
    save_release_radar_week,
    get_week_key
)

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
    
    # Get current week key for history
    week_key = get_week_key()
    log.info(f"Processing releases for week: {week_key}")
    
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
            process_release_radar_user(user, session, week_key) 
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


async def process_release_radar_user(
    user: dict, 
    session: aiohttp.ClientSession,
    week_key: str
) -> dict:
    """
    Process a single user's release radar.
    
    Args:
        user: User dict with email, refreshToken, etc.
        session: aiohttp session for API calls
        week_key: Current week key for history storage
        
    Returns:
        Dict with email, track count, artist count, and releases
        
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
        
        # Get new releases (this can take a while)
        log.info(f"[{email}] Scanning for new releases...")
        await spotify.followed_artists.aiohttp_get_followed_artist_latest_release()
        
        track_uris = spotify.followed_artists.artist_tracks.final_tracks_uris
        track_count = len(track_uris)
        log.info(f"[{email}] Found {track_count} new tracks this week")
        
        # Get release details for history storage
        # We need album-level info, not just track URIs
        releases = await get_release_details_for_history(
            spotify, 
            spotify.followed_artists.artist_tracks.album_uri_list
        )
        
        # Create or update playlist
        playlist_id = spotify.release_radar_playlist.id
        
        if not playlist_id:
            log.info(f"[{email}] Creating new Release Radar playlist...")
            await spotify.release_radar_playlist.aiohttp_build_playlist(
                track_uris,
                BLACK_LOGO_BASE_64
            )
            playlist_id = spotify.release_radar_playlist.id
            # Save playlist ID to user record
            update_user_table_release_radar_id(user, playlist_id)
            log.info(f"[{email}] Created playlist: {playlist_id}")
        else:
            log.info(f"[{email}] Updating existing playlist: {playlist_id}")
            await spotify.release_radar_playlist.aiohttp_update_playlist(track_uris)
        
        # Save to history table
        log.info(f"[{email}] Saving release radar history...")
        save_release_radar_week(
            email=email,
            week_key=week_key,
            releases=releases,
            playlist_id=playlist_id
        )
        
        log.info(f"[{email}] ✅ Release radar complete!")
        return {
            "email": email, 
            "tracks": track_count,
            "artists": artist_count,
            "releases": releases,
            "playlistId": playlist_id,
            "weekKey": week_key
        }
        
    except Exception as err:
        log.error(f"[{email}] ❌ Failed: {err}")
        raise ReleaseRadarError(
            message=f"Process user {email} failed: {err}",
            function="process_release_radar_user"
        )


async def get_release_details_for_history(spotify, album_uris: list) -> list:
    """
    Get detailed release information for storing in history.
    
    Args:
        spotify: Spotify client instance
        album_uris: List of album URIs
        
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
                from lambdas.common.aiohttp_helper import fetch_json
                data = await fetch_json(
                    spotify.aiohttp_session, 
                    url, 
                    headers=spotify.headers
                )
                
                for album in data.get('albums', []):
                    if not album:
                        continue
                    
                    releases.append({
                        'id': album.get('id'),
                        'name': album.get('name'),
                        'artists': [
                            {'id': a.get('id'), 'name': a.get('name')} 
                            for a in album.get('artists', [])
                        ],
                        'images': album.get('images', []),
                        'album_type': album.get('album_type'),
                        'release_date': album.get('release_date'),
                        'total_tracks': album.get('total_tracks', 1),
                        'uri': album.get('uri'),
                        'external_urls': album.get('external_urls', {})
                    })
                    
            except Exception as err:
                log.warning(f"Failed to fetch album batch: {err}")
                continue
        
        return releases
        
    except Exception as err:
        log.error(f"Get release details failed: {err}")
        return []
