import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

from lambdas.common.wrapped_helper import get_active_wrapped_users
from lambdas.common.spotify import Spotify
from lambdas.common.constants import FROM_EMAIL, XOMIFY_URL
from lambdas.common.logger import get_logger
from lambdas.common.dynamo_helpers import get_user_wrap_by_month
from lambdas.common.ses_helper import send_wrapped_email
from email_template import generate_email_html

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


def get_month_display_name(month_key: str) -> str:
    """
    Convert month key to display name.
    e.g., "2024-12" -> "December 2024"
    """
    year, month = month_key.split('-')
    return datetime(int(year), int(month), 1).strftime('%B %Y')


async def wrapped_email_cron_job(event):
    """
    Main entry point for the monthly wrapped email cron job.
    Sends personalized emails to all enrolled users with their wrapped preview.
    """
    try:
        log.info("=" * 50)
        log.info("Starting Wrapped Email Cron Job...")
        log.info("=" * 50)
        
        # Get all active wrapped users
        wrapped_users = get_active_wrapped_users()
        log.info(f"Found {len(wrapped_users)} active wrapped users")
        
        if not wrapped_users:
            log.info("No active users to email.")
            return [], []

        # Get the month key for this run (last month)
        month_key = get_last_month_key()
        month_name = get_month_display_name(month_key)
        log.info(f"Sending wrapped emails for: {month_name} ({month_key})")

        # Use a single session for all Spotify API calls
        connector = aiohttp.TCPConnector(limit=10)
        timeout = aiohttp.ClientTimeout(total=300)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = [
                process_user_email(user, session, month_key, month_name) 
                for user in wrapped_users
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Separate successes from failures
        successes = []
        failures = []
        
        for user, result in zip(wrapped_users, results):
            if isinstance(result, Exception):
                log.error(f"❌ User {user['email']} failed: {result}")
                failures.append({"email": user['email'], "error": str(result)})
            else:
                log.info(f"✅ User {result} email sent successfully")
                successes.append(result)

        log.info("=" * 50)
        log.info(f"Wrapped Email Cron Job Complete!")
        log.info(f"Emails Sent: {len(successes)}, Failed: {len(failures)}")
        log.info("=" * 50)
        
        return successes, failures
        
    except Exception as err:
        log.error(f"Wrapped Email Cron Job: {err}")
        raise Exception(f"Wrapped Email Cron Job: {err}") from err


async def process_user_email(user: dict, session: aiohttp.ClientSession, month_key: str, month_name: str):
    """
    Process a single user's wrapped email.
    Fetches their data and sends personalized email.
    """
    email = user.get('email', 'unknown')
    
    try:
        log.info(f"[{email}] Processing email...")
        
        # Check if user has opted out of emails
        if user.get('emailOptOut', False):
            log.info(f"[{email}] User opted out of emails, skipping")
            return email  # Still count as "success" - just skipped
        
        # Get their wrapped data for last month
        wrapped_data = get_user_wrap_by_month(email, month_key)
        
        if not wrapped_data:
            log.info(f"[{email}] No wrapped data for {month_key}, skipping")
            return email
        
        # Initialize Spotify client to fetch track/artist names
        spotify = Spotify(user, session)
        access_token = await spotify.aiohttp_get_access_token()
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Extract top 5 IDs (short_term is most recent listening)
        top_song_ids = wrapped_data.get('topSongIds', {}).get('short_term', [])[:5]
        top_artist_ids = wrapped_data.get('topArtistIds', {}).get('short_term', [])[:5]
        top_genres_dict = wrapped_data.get('topGenres', {}).get('short_term', {})
        
        # Convert genres dict to sorted list
        if isinstance(top_genres_dict, dict):
            top_genres_list = sorted(top_genres_dict.items(), key=lambda x: x[1], reverse=True)[:5]
            top_genres = [g[0].title() for g in top_genres_list]  # Capitalize genre names
        else:
            top_genres = []
        
        # Fetch actual track/artist names from Spotify
        top_songs = await fetch_track_names(session, headers, top_song_ids)
        top_artists = await fetch_artist_names(session, headers, top_artist_ids)
        
        log.info(f"[{email}] Top songs: {top_songs}")
        log.info(f"[{email}] Top artists: {top_artists}")
        log.info(f"[{email}] Top genres: {top_genres}")
        
        # Generate email HTML
        html_body = generate_email_html(
            month_name=month_name,
            top_songs=top_songs,
            top_artists=top_artists,
            top_genres=top_genres,
            xomify_url=XOMIFY_URL,
            unsubscribe_url=f"{XOMIFY_URL}/unsubscribe?email={email}"
        )
        
        # Generate plain text version
        text_body = generate_plain_text_email(
            month_name=month_name,
            top_songs=top_songs,
            top_artists=top_artists,
            top_genres=top_genres
        )
        
        # Send the email
        subject = f"🎵 Your {month_name} Wrapped is Ready!"
        send_wrapped_email(
            to_email=email,
            subject=subject,
            html_body=html_body,
            text_body=text_body
        )
        
        log.info(f"[{email}] ✅ Email sent!")
        return email
        
    except Exception as err:
        log.error(f"[{email}] ❌ Failed: {err}")
        raise Exception(f"Process user email {email} failed: {err}") from err


async def fetch_track_names(session: aiohttp.ClientSession, headers: dict, track_ids: list) -> list:
    """
    Fetch track names from Spotify API.
    Returns list of "Track Name - Artist Name" strings.
    """
    if not track_ids:
        return []
    
    try:
        ids_str = ','.join(track_ids[:50])
        url = f"https://api.spotify.com/v1/tracks?ids={ids_str}"
        
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                log.warning(f"Failed to fetch tracks: {resp.status}")
                return track_ids  # Return IDs as fallback
            
            data = await resp.json()
            tracks = data.get('tracks', [])
            
            return [
                f"{track['name']} - {', '.join([a['name'] for a in track['artists']])}"
                for track in tracks if track
            ]
    except Exception as err:
        log.error(f"Fetch Track Names: {err}")
        return track_ids  # Return IDs as fallback


async def fetch_artist_names(session: aiohttp.ClientSession, headers: dict, artist_ids: list) -> list:
    """
    Fetch artist names from Spotify API.
    Returns list of artist name strings.
    """
    if not artist_ids:
        return []
    
    try:
        ids_str = ','.join(artist_ids[:50])
        url = f"https://api.spotify.com/v1/artists?ids={ids_str}"
        
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                log.warning(f"Failed to fetch artists: {resp.status}")
                return artist_ids  # Return IDs as fallback
            
            data = await resp.json()
            artists = data.get('artists', [])
            
            return [artist['name'] for artist in artists if artist]
    except Exception as err:
        log.error(f"Fetch Artist Names: {err}")
        return artist_ids  # Return IDs as fallback


def generate_plain_text_email(month_name: str, top_songs: list, top_artists: list, top_genres: list) -> str:
    """
    Generate plain text version of the email.
    """
    songs_text = '\n'.join([f"  {i+1}. {song}" for i, song in enumerate(top_songs)]) or "  No data yet"
    artists_text = '\n'.join([f"  {i+1}. {artist}" for i, artist in enumerate(top_artists)]) or "  No data yet"
    genres_text = '\n'.join([f"  {i+1}. {genre}" for i, genre in enumerate(top_genres)]) or "  No data yet"
    
    return f"""
Your Xomify Wrapped for {month_name} is ready!

🎵 Your Top Songs:
{songs_text}

🎤 Your Top Artists:
{artists_text}

🎧 Your Top Genres:
{genres_text}

View your full Wrapped: {XOMIFY_URL}/wrapped

---
You're receiving this because you're enrolled in Xomify Wrapped.
Unsubscribe: {XOMIFY_URL}/unsubscribe
    """.strip()
