"""
XOMIFY Release Radar History Backfill
=====================================
Backfills 6 months of release radar history for new users.

Week Definition: Sunday 00:00:00 to Saturday 23:59:59
- Only saves COMPLETED weeks (not current incomplete week)
- All backfilled weeks are marked as finalized=True

Triggered when:
1. User enrolls in release radar (via API)
2. User visits release radar page with no history (via API)
"""

import asyncio
import aiohttp
from datetime import datetime, timedelta

from lambdas.common.logger import get_logger
from lambdas.common.errors import ReleaseRadarError
from lambdas.common.spotify import Spotify
from lambdas.common.aiohttp_helper import fetch_json
from lambdas.common.release_radar_dynamo import (
    get_week_key,
    save_release_radar_week,
    check_user_has_history
)

log = get_logger(__file__)


async def backfill_release_radar_history(user: dict) -> dict:
    """
    Backfill 6 months of release radar history for a user.
    
    Only backfills COMPLETED weeks (not current incomplete week).
    All backfilled weeks are marked as finalized=True.
    
    Args:
        user: User dict with email, refreshToken, etc.
        
    Returns:
        Dict with backfill results
    """
    email = user.get('email', 'unknown')
    
    log.info(f"[{email}] Starting 6-month history backfill...")
    
    # Check if user already has finalized history
    if check_user_has_history(email, finalized_only=True):
        log.info(f"[{email}] User already has finalized history, skipping backfill")
        return {"email": email, "status": "skipped", "reason": "history_exists"}
    
    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=300)  # 5 min timeout
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        try:
            # Initialize Spotify client
            spotify = Spotify(user, session)
            await spotify.aiohttp_initialize_release_radar()
            
            # Get ALL followed artists
            log.info(f"[{email}] Fetching followed artists...")
            await spotify.followed_artists.aiohttp_get_followed_artists()
            artist_ids = spotify.followed_artists.artist_id_list
            log.info(f"[{email}] Found {len(artist_ids)} followed artists")
            
            if not artist_ids:
                return {"email": email, "status": "skipped", "reason": "no_followed_artists"}
            
            # Fetch all releases from last 6 months
            log.info(f"[{email}] Fetching releases from last 6 months...")
            all_releases = await fetch_all_releases_for_backfill(
                spotify, 
                artist_ids, 
                months=6
            )
            log.info(f"[{email}] Found {len(all_releases)} total releases")
            
            # Group releases by Sunday-Saturday week
            releases_by_week = group_releases_by_week(all_releases)
            log.info(f"[{email}] Grouped into {len(releases_by_week)} weeks")
            
            # Get current week key to exclude it (it's incomplete)
            current_week_key = get_week_key()
            
            # Save each COMPLETED week to DynamoDB
            weeks_saved = 0
            for week_key, releases in releases_by_week.items():
                # Skip current incomplete week
                if week_key == current_week_key:
                    log.info(f"[{email}] Skipping current incomplete week {week_key}")
                    continue
                
                try:
                    save_release_radar_week(
                        email=email,
                        week_key=week_key,
                        releases=releases,
                        playlist_id=None,  # No playlist for historical weeks
                        finalized=True  # Backfilled weeks are finalized
                    )
                    weeks_saved += 1
                except Exception as err:
                    log.warning(f"[{email}] Failed to save week {week_key}: {err}")
            
            log.info(f"[{email}] ✅ Backfill complete! Saved {weeks_saved} weeks")
            
            return {
                "email": email,
                "status": "success",
                "weeksBackfilled": weeks_saved,
                "totalReleases": len(all_releases)
            }
            
        except Exception as err:
            log.error(f"[{email}] ❌ Backfill failed: {err}")
            raise ReleaseRadarError(
                message=f"Backfill failed for {email}: {err}",
                function="backfill_release_radar_history"
            )


async def fetch_all_releases_for_backfill(
    spotify, 
    artist_ids: list, 
    months: int = 6
) -> list:
    """
    Fetch all releases from followed artists within the time window.
    
    Args:
        spotify: Spotify client instance
        artist_ids: List of artist IDs
        months: Number of months to look back
        
    Returns:
        List of release objects with full details
    """
    all_releases = []
    seen_ids = set()
    
    cutoff_date = datetime.now() - timedelta(days=months * 30)
    
    # Process artists in batches
    batch_size = 20
    release_types = ['album', 'single', 'appears_on']
    
    for i in range(0, len(artist_ids), batch_size):
        batch = artist_ids[i:i+batch_size]
        
        for artist_id in batch:
            for release_type in release_types:
                try:
                    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
                    url += f"?include_groups={release_type}&limit=20"
                    
                    data = await fetch_json(
                        spotify.aiohttp_session,
                        url,
                        headers=spotify.headers
                    )
                    
                    for album in data.get('items', []):
                        album_id = album.get('id')
                        if album_id in seen_ids:
                            continue
                        
                        # Parse release date
                        release_date_str = album.get('release_date', '')
                        try:
                            if len(release_date_str) == 4:
                                release_date = datetime(int(release_date_str), 1, 1)
                            elif len(release_date_str) == 7:
                                release_date = datetime.strptime(release_date_str, '%Y-%m')
                            else:
                                release_date = datetime.strptime(release_date_str[:10], '%Y-%m-%d')
                        except:
                            continue
                        
                        # Check if within time window
                        if release_date < cutoff_date:
                            continue
                        
                        seen_ids.add(album_id)
                        
                        # Normalize for storage
                        all_releases.append({
                            'id': album_id,
                            'name': album.get('name'),
                            'artistName': album.get('artists', [{}])[0].get('name', 'Unknown'),
                            'artistId': album.get('artists', [{}])[0].get('id'),
                            'imageUrl': album.get('images', [{}])[0].get('url') if album.get('images') else None,
                            'albumType': album.get('album_type'),
                            'releaseDate': release_date_str,
                            'release_date_parsed': release_date,
                            'totalTracks': album.get('total_tracks', 1),
                            'uri': album.get('uri')
                        })
                        
                except Exception as err:
                    log.debug(f"Failed to fetch {release_type} for artist {artist_id}: {err}")
                    continue
        
        # Small delay between batches
        if i + batch_size < len(artist_ids):
            await asyncio.sleep(0.3)
    
    return all_releases


def group_releases_by_week(releases: list) -> dict:
    """
    Group releases by their week key (Sunday-Saturday weeks).
    
    Args:
        releases: List of release objects with release_date_parsed
        
    Returns:
        Dict mapping week_key -> list of releases
    """
    weeks = {}
    
    for release in releases:
        release_date = release.get('release_date_parsed')
        if not release_date:
            continue
        
        # Get week key for this release (Sunday-Saturday)
        week_key = get_week_key(release_date)
        
        if week_key not in weeks:
            weeks[week_key] = []
        
        # Remove the parsed date before storing (not JSON serializable)
        release_copy = {k: v for k, v in release.items() if k != 'release_date_parsed'}
        weeks[week_key].append(release_copy)
    
    return weeks


# Lambda handler
def handler(event, context):
    """AWS Lambda entry point."""
    try:
        user = event.get('user')
        if not user:
            return {
                'statusCode': 400,
                'body': {'error': 'Missing user data'}
            }
        
        result = asyncio.run(backfill_release_radar_history(user))
        
        return {
            'statusCode': 200,
            'body': result
        }
        
    except Exception as err:
        log.error(f"Lambda handler error: {err}")
        return {
            'statusCode': 500,
            'body': {'error': str(err)}
        }


# For direct invocation/testing
async def run_backfill(user: dict):
    """Run backfill directly (for testing)."""
    return await backfill_release_radar_history(user)
