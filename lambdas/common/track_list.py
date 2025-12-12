import requests
import aiohttp
import time
from datetime import datetime
import asyncio
from lambdas.common.aiohttp_helper import fetch_json
from lambdas.common.constants import LOGGER

log = LOGGER.get_logger(__file__)

class TrackList:

    BASE_URL = "https://api.spotify.com/v1"

    def __init__(self, term: str, headers: dict, session: aiohttp.ClientSession = None):
        log.info(f"Initializing Tracks for term: {term}")
        self.aiohttp_session = session
        self.term: str = term
        self.headers = headers
        self.track_list: list = []
        self.track_uri_list: list = []
        self.album_uri_list: list = []
        self.final_tracks_uris: list = []
        self.track_id_list: list = []
        self.number_of_tracks: int = 0
    
    # ------------------------
    # Shared Methods
    # ------------------------
    def __get_uri_list(self):
        return [track['uri'] for track in self.track_list if 'uri' in track]
    def __get_id_list(self):
        return [track['id'] for track in self.track_list if 'id' in track]
    
    # ------------------------
    # Set Tracks
    # ------------------------
    async def set_top_tracks(self):
        try:
            log.info(f"Setting Top Tracks for term: {self.term}")
            self.track_list = await self.get_top_tracks()
            self.track_uri_list = self.__get_uri_list()
            self.track_id_list = self.__get_id_list()
            self.number_of_tracks = len(self.track_list)
            log.info(f"{self.number_of_tracks} Top Tracks Set successfully for term {self.term}!")
        except Exception as err:
            log.error(f"Set User Top Tracks: {err}")
            raise Exception(f"Set User Top Tracks {self.term}: {err}") from err
        
    async def aiohttp_set_top_tracks(self):
        try:
            log.info(f"Setting Top Tracks for term: {self.term}")
            self.track_list = await self.aiohttp_get_top_tracks()
            self.track_uri_list = self.__get_uri_list()
            self.track_id_list = self.__get_id_list()
            self.number_of_tracks = len(self.track_list)
            log.info(f"{self.number_of_tracks} Top Tracks Set successfully for term {self.term}!")
        except Exception as err:
            log.error(f"AIOHTTP Set User Top Tracks: {err}")
            raise Exception(f"AIOHTTP Set User Top Tracks {self.term}: {err}") from err

    # ------------------------
    # Get Tracks
    # ------------------------
    async def get_top_tracks(self):
        try:
            log.info(f"Getting top tracks for term {self.term}...")
            url = f"{self.BASE_URL}/me/top/tracks?limit=25&time_range={self.term}"

            # Make the request
            response = requests.get(url, headers=self.headers)
            response_data = response.json()

            # Check for errors
            if response.status_code != 200:
                raise Exception(f"Error fetching top tracks: {response_data}")

            return response_data['items']  # Return the list of top tracks
        except Exception as err:
            log.error(f"Get User Top Tracks: {err}")
            raise Exception(f"Get User Top Tracks {self.term}: {err}") from err
        
    async def aiohttp_get_top_tracks(self):
        try:
            log.info(f"Getting top tracks for term {self.term}...")
            url = f"{self.BASE_URL}/me/top/tracks?limit=25&time_range={self.term}"

            data = await fetch_json(self.aiohttp_session, url, headers=self.headers)
            return data['items']
        except Exception as err:
            log.error(f"AIOHTTP Get User Top Tracks: {err}")
            raise Exception(f"AIOHTTP Get User Top Tracks {self.term}: {err}") from err
    
    # ------------------------
    # Get Latest Release (FIXED)
    # ------------------------
    async def get_artist_latest_release(self, artist_id_list: list):
        try:
            log.info("Getting artist releases within the last week...")
            tasks = [self.get_latest_releases(artist_id) for artist_id in artist_id_list]
            artist_latest_release_uris = await asyncio.gather(*tasks)
            combined_artist_latest_release_uris = [item for sublist in artist_latest_release_uris for item in sublist]
            
            log.info(f"Found {len(combined_artist_latest_release_uris)} new release URIs")
            
            # Split into tracks and albums, remove duplicates
            self.track_uri_list, temp_album_uri_list = self.__split_spotify_uris(combined_artist_latest_release_uris)
            self.album_uri_list = list(set(temp_album_uri_list))
            
            log.info(f"New Albums: {len(self.album_uri_list)}, Direct Tracks: {len(self.track_uri_list)}")

            # Get all tracks from new albums
            if self.album_uri_list:
                all_tracks_from_albums_uris = await self.get_several_albums_tracks()
                self.track_uri_list.extend(all_tracks_from_albums_uris)
            
            # Remove duplicates from final list
            self.final_tracks_uris = list(set(self.track_uri_list))
            log.info(f"Total unique tracks for release radar: {len(self.final_tracks_uris)}")
        except Exception as err:
            log.error(f"Get Artist Latest Release: {err}")
            raise Exception(f"Get Artist Latest Release: {err}") from err
    
    async def aiohttp_get_artist_latest_release(self, artist_id_list: list):
        try:
            log.info(f"Getting releases for {len(artist_id_list)} followed artists...")
            
            # Process in batches to avoid overwhelming the API
            batch_size = 20
            all_release_uris = []
            
            for i in range(0, len(artist_id_list), batch_size):
                batch = artist_id_list[i:i + batch_size]
                tasks = [self.aiohttp_get_latest_releases(artist_id) for artist_id in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, Exception):
                        log.warning(f"Failed to get releases for an artist: {result}")
                    else:
                        all_release_uris.extend(result)
                
                # Small delay between batches to be nice to the API
                if i + batch_size < len(artist_id_list):
                    await asyncio.sleep(0.5)
            
            log.info(f"Found {len(all_release_uris)} new release URIs from all artists")

            # Split into tracks and albums
            self.track_uri_list, temp_album_uri_list = self.__split_spotify_uris(all_release_uris)
            self.album_uri_list = list(set(temp_album_uri_list))
            
            log.info(f"New Albums: {len(self.album_uri_list)}, Direct Tracks: {len(self.track_uri_list)}")

            # Get all tracks from new albums
            if self.album_uri_list:
                all_tracks_from_albums_uris = await self.aiohttp_get_several_albums_tracks()
                self.track_uri_list.extend(all_tracks_from_albums_uris)

            # Remove duplicates
            self.final_tracks_uris = list(set(self.track_uri_list))
            log.info(f"Total unique tracks for release radar: {len(self.final_tracks_uris)}")
        except Exception as err:
            log.error(f"AIOHTTP Get Artist Latest Release: {err}")
            raise Exception(f"AIOHTTP Get Artist Latest Release: {err}") from err
    
    async def get_latest_releases(self, artist_id: str):
        """
        Get all releases from an artist within the last 7 days.
        Fetches multiple albums at once instead of one-by-one recursion.
        """
        try:
            # FIXED: Removed quotes around include_groups, fixed URL format, increased limit
            include_groups = "album,single,appears_on,compilation"
            url = f"{self.BASE_URL}/artists/{artist_id}/albums?include_groups={include_groups}&limit=10"

            response = requests.get(url, headers=self.headers)
            response_data = response.json()

            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 1))
                log.warning(f"Rate limit reached. Retrying after {retry_after} seconds...")
                time.sleep(retry_after + 1)
                return await self.get_latest_releases(artist_id)
            
            if response.status_code != 200:
                raise Exception(f"Error fetching artist albums: {response_data}")
            
            release_uris = []
            for release in response_data.get('items', []):
                release_date = release.get('release_date', '')
                if self.__is_within_a_week(release_date):
                    log.debug(f"New release: {release['name']} ({release_date})")
                    release_uris.append(release['uri'])
            
            return release_uris

        except Exception as err:
            log.error(f"Get Latest Releases for artist {artist_id}: {err}")
            return []  # Return empty list instead of raising to not break other artists
    
    async def aiohttp_get_latest_releases(self, artist_id: str):
        """
        Get all releases from an artist within the last 7 days.
        FIXED: Proper URL formatting, fetch multiple at once.
        """
        try:
            # FIXED: Removed quotes, fixed URL params, increased limit to catch more releases
            include_groups = "album,single,appears_on,compilation"
            url = f"{self.BASE_URL}/artists/{artist_id}/albums?include_groups={include_groups}&limit=10"

            data = await fetch_json(self.aiohttp_session, url, headers=self.headers)

            release_uris = []
            for release in data.get('items', []):
                release_date = release.get('release_date', '')
                if self.__is_within_a_week(release_date):
                    log.debug(f"New release found: {release['name']} - {release_date}")
                    release_uris.append(release['uri'])
            
            return release_uris
        except Exception as err:
            log.error(f"AIOHTTP Get Latest Releases for artist {artist_id}: {err}")
            return []  # Return empty instead of raising to not break the whole batch
    
    # ------------------------
    # Get Tracks from Album (FIXED)
    # ------------------------
    async def get_album_tracks(self, album_uri: str):
        try:
            album_id = album_uri.split(":")[2]
            url = f"{self.BASE_URL}/albums/{album_id}/tracks?limit=50"

            response = requests.get(url, headers=self.headers)
            response_data = response.json()

            if response.status_code != 200:
                raise Exception(f"Error fetching Album tracks: {response_data}")
                
            return [track['uri'] for track in response_data.get('items', [])]
        except Exception as err:
            log.error(f"Get Album Tracks: {err}")
            raise Exception(f"Get Album Tracks: {err}") from err
    
    async def aiohttp_get_album_tracks(self, album_uri: str):
        try:
            album_id = album_uri.split(":")[2]
            url = f"{self.BASE_URL}/albums/{album_id}/tracks?limit=50"
            data = await fetch_json(self.aiohttp_session, url, headers=self.headers)
            return [track['uri'] for track in data.get('items', [])]
        except Exception as err:
            log.error(f"AIOHTTP Get Album Tracks: {err}")
            raise Exception(f"AIOHTTP Get Album Tracks: {err}") from err
        
    async def get_several_albums_tracks(self):
        """
        Batch fetch album details and extract all tracks.
        FIXED: Now gets ALL tracks from singles too, not just the first one.
        """
        try:
            album_ids = [uri.split(":")[2] for uri in self.album_uri_list]
            track_uris = []

            # Spotify allows up to 20 album IDs per request
            for i in range(0, len(album_ids), 20):
                batch_ids = album_ids[i:i+20]
                ids_param = ",".join(batch_ids)
                url = f"{self.BASE_URL}/albums?ids={ids_param}"

                response = requests.get(url, headers=self.headers)
                response_data = response.json()

                if response.status_code != 200:
                    raise Exception(f"Error fetching albums: {response_data}")

                for album in response_data.get("albums", []):
                    if not album or "tracks" not in album:
                        continue
                    # FIXED: Get ALL tracks from the album, regardless of type
                    for track in album["tracks"].get("items", []):
                        track_uris.append(track["uri"])

            return track_uris
        except Exception as err:
            log.error(f"Get Several Albums Tracks: {err}")
            raise Exception(f"Get Several Albums Tracks: {err}") from err
    
    async def aiohttp_get_several_albums_tracks(self):
        """
        Batch fetch album details and extract all tracks.
        FIXED: Now gets ALL tracks from all album types.
        """
        try:
            album_ids = [uri.split(":")[2] for uri in self.album_uri_list]
            track_uris = []
            
            log.info(f"Fetching tracks from {len(album_ids)} albums...")
            
            for i in range(0, len(album_ids), 20):
                batch_ids = album_ids[i:i+20]
                ids_param = ",".join(batch_ids)
                url = f"{self.BASE_URL}/albums?ids={ids_param}"
                
                data = await fetch_json(self.aiohttp_session, url, headers=self.headers)
                
                for album in data.get("albums", []):
                    if not album or "tracks" not in album:
                        continue
                    
                    album_name = album.get('name', 'Unknown')
                    album_tracks = album["tracks"].get("items", [])
                    
                    # FIXED: Get ALL tracks from every album type
                    for track in album_tracks:
                        track_uris.append(track["uri"])
                    
                    log.debug(f"Album '{album_name}': {len(album_tracks)} tracks added")

            log.info(f"Total tracks extracted from albums: {len(track_uris)}")
            return track_uris
        except Exception as err:
            log.error(f"AIOHTTP Get Several Albums Tracks: {err}")
            raise Exception(f"AIOHTTP Get Several Albums Tracks: {err}") from err

    # ------------------------
    # Helper Functions
    # ------------------------
    def __is_within_a_week(self, target_date_str: str) -> bool:
        """Check if a date string is within the last 7 days."""
        try:
            if not target_date_str or len(target_date_str) < 4:
                return False
            
            today = datetime.today().date()
            
            # Handle different date formats from Spotify
            # Could be: "2024" (year only), "2024-01" (year-month), "2024-01-15" (full date)
            if len(target_date_str) == 4:  # Year only
                return False  # Can't determine week from year only
            elif len(target_date_str) == 7:  # Year-month
                # Check if it's current month
                target_date = datetime.strptime(target_date_str, '%Y-%m')
                return (today.year == target_date.year and today.month == target_date.month)
            else:  # Full date
                target_date = datetime.strptime(target_date_str[:10], '%Y-%m-%d').date()
                difference_in_days = (today - target_date).days
                return 0 <= difference_in_days <= 7
                
        except Exception as err:
            log.warning(f"Error parsing date '{target_date_str}': {err}")
            return False
        
    def __split_spotify_uris(self, uris: list) -> tuple:
        """Split URIs into tracks and albums."""
        tracks = [uri for uri in uris if uri and uri.startswith("spotify:track:")]
        albums = [uri for uri in uris if uri and uri.startswith("spotify:album:")]
        return tracks, albums