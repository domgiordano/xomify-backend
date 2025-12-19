

from datetime import datetime, timezone
import asyncio

from lambdas.common.wrapped_helper import get_active_wrapped_users
from lambdas.common.spotify import Spotify
from lambdas.common.constants import USERS_TABLE_NAME, LOGO_BASE_64, BLACK_2025_BASE_64
from lambdas.common.logger import get_logger
from lambdas.common.dynamo_helpers import update_table_item

log = get_logger(__file__)

async def wrapped_chron_job(event):
    try:
        log.info("Starting Wrapped Chron Job...")
        response = []
        wrapped_users = get_active_wrapped_users()
        for user in wrapped_users:
            log.info(f"Found User: {user}")
            spotify = Spotify(user)

            await spotify.get_top_tracks()
            await spotify.get_top_artists()

            tasks = [
                spotify.monthly_spotify_playlist.build_playlist(spotify.top_tracks_short.track_uri_list, LOGO_BASE_64)
            ]
            if spotify.last_month_number == 6:
                tasks.append(spotify.first_half_of_year_spotify_playlist.build_playlist(spotify.top_tracks_medium.track_uri_list, LOGO_BASE_64))

            if spotify.last_month_number == 12:
                tasks.append(spotify.full_year_spotify_playlist.build_playlist(spotify.top_tracks_long.track_uri_list, BLACK_2025_BASE_64))

            await asyncio.gather(*tasks)

            # Create Dicts
            log.info("Getting last months top tracks, artists, and genres...")
            top_tracks_last_month = spotify.get_top_tracks_ids_last_month()
            top_artists_last_month = spotify.get_top_artists_ids_last_month()
            top_genres_last_month = spotify.get_top_genres_last_month()

            # Update the User
            log.info("Updating User table with data...")
            __update_user_table_entry(user, top_tracks_last_month, top_artists_last_month, top_genres_last_month)

            response.append(spotify.email)

            log.info(f"---------- USER COMPLETE: {spotify.email} ----------")

        return response
    except Exception as err:
        log.error(f"Wrapped Chron Job: {err}")
        raise Exception(f"Wrapped Chron Job: {err}")

def __update_user_table_entry(user, top_tracks_last_month, top_artists_last_month, top_genres_last_month):
    # Tracks
    user['topSongIdsTwoMonthsAgo'] = user.get('topSongIdsLastMonth', {})
    user['topSongIdsLastMonth'] = top_tracks_last_month
    # Artists
    user['topArtistIdsTwoMonthsAgo'] = user.get('topArtistIdsLastMonth', {})
    user['topArtistIdsLastMonth'] = top_artists_last_month
    # Genres
    user['topGenresTwoMonthsAgo'] = user.get('topGenresLastMonth', {})
    user['topGenresLastMonth'] = top_genres_last_month
    # Time Stamp
    user['updatedAt'] = get_time_stamp()
    update_table_item(USERS_TABLE_NAME, user)

def get_time_stamp():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


