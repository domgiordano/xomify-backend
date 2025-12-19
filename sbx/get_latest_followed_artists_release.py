import requests
import asyncio
import time
from datetime import datetime
from priv_constants import DOM_REFRESH_TOKEN
from get_user_with_rerfresh_token import get_access_token, get_user

BASE_URL = "https://api.spotify.com/v1"

def get_followed_artists(headers: dict):
    try:
        print(f"Getting followed artists..")
        artist_ids = []

        url = f"{BASE_URL}/me/following?type=artist&limit=50"
        more_artists = True
        while(more_artists):
            
            # Make the request
            response = requests.get(url, headers=headers)
            response_data = response.json()

            # Check for errors
            if response.status_code != 200:
                raise Exception(f"Error fetching followed artists: {response_data}")

            ids = [{artist['name']: artist['id']} for artist in response_data['artists']['items']]
            artist_ids.extend(ids)
            if not response_data['artists']['next']:
                more_artists = False
            else:
                url = response_data['artists']['next']
        print("Followed Artists retrieved successfully!")
        return artist_ids

    except Exception as err:
        print(f"Get Followed Artists: {err}")
        raise Exception(f"Get Followed Artists: {err}")

async def get_artist_latest_release(artist_id_list: list, headers: dict):
    print("Getting artsist releases within the last week...")
    tasks = [get_latest_releases(id, headers) for id in artist_id_list]
    print("TASKS------")
    print(tasks)
    # Get all ids of latest releases for the week
    artist_latest_release_uris = await asyncio.gather(*tasks)
    print("RESUTL =========")
    print(artist_latest_release_uris)
    combined_artist_latest_release_uris = [item for sublist in artist_latest_release_uris for item in sublist]
    print(f"Latest Release IDs: {combined_artist_latest_release_uris}")
    print(len(combined_artist_latest_release_uris))
    # Remove None values - split lists
    track_uri_list, album_uri_list = __split_spotify_uris(combined_artist_latest_release_uris)
    print(f"Latest Release Albums: {album_uri_list}")
    print(len(album_uri_list))
    print(f"Latest Release Tracks: {track_uri_list}")
    print(len(track_uri_list))
    final_album_uri_list = list(set(album_uri_list))
    print(final_album_uri_list)

    # # Get all tracks for new albums
    # all_tracks_from_albums_uris = await self.get_several_albums_tracks()
    # print(f"All Tracks from Albums: {all_tracks_from_albums_uris}")
    # print(len(all_tracks_from_albums_uris))
    # self.track_uri_list.extend(all_tracks_from_albums_uris)
    # # Remove Duplicates
    # self.final_tracks_uris = list(set(self.track_uri_list))
    # print(f"All Tracks total: {len(self.final_tracks_uris)}")

async def get_latest_releases(artist_id, headers):
    try:
        include_groups = "album,single,appears_on,compilation"
        url = f"{BASE_URL}/artists/{artist_id}/albums?&include_groups='{include_groups}'&limit=3&offset=0"

        # Make the request
        response = requests.get(url, headers=headers)
        response_data = response.json()

        # Check for errors
        if response.status_code == 429:
            print("RATE LIMIT REACHED")
            time.sleep(response.headers['retry-after'] + 1)
            return await get_artist_latest_release(artist_id)
        if response.status_code != 200:
            raise Exception(f"Error fetching artist latest release: {response}")
        
        release_uris = []
        for release in response_data['items']:
            for artist in release['artists']:
                print(f"Artist: {artist['name']}")
            print(f"Album: {release['name']}")
            print(f"Release Date: {release['release_date']}")
            if __is_within_a_week(release['release_date']):
                print("New Release Added.")
                release_uris.append(release['uri'])
            else:
                print("Old Release Skipped.")
            print("\n")
        return release_uris

    except Exception as err:
        print(f"Get Artist Latest Release: {err}")
        raise Exception(f"Get Artist Latest Release: {err}")
    
def __is_within_a_week(target_date_str: str):
    try:
        if len(target_date_str) < 8:
            return False
        today = datetime.today().date()
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()

        # Calculate the absolute difference in days
        difference_in_days = abs((today - target_date).days)
        print("DIFFERENCE IN DAYS WITHIN A WEEK??")
        print(difference_in_days < 7)
        return difference_in_days < 7
    except Exception as err:
        print(f"Is Date Within a week: {err}")
        raise Exception(f"Is Date Within a week: {err}")
        
def __split_spotify_uris(uris):
    tracks = [id for id in uris if id and id.startswith("spotify:track:")]
    albums = [id for id in uris if id and id.startswith("spotify:album:")]
    return tracks, albums





