import requests
import aiohttp
from lambdas.common.track_list import TrackList
from lambdas.common.constants import LOGGER
from lambdas.common.aiohttp_helper import fetch_json

log = LOGGER.get_logger(__file__)

class ArtistList:

    BASE_URL = "https://api.spotify.com/v1"

    def __init__(self, term: str, headers: dict, session: aiohttp.ClientSession = None):
        log.info(f"Initializing Artist for term: {term}")
        self.aiohttp_session = session
        self.term: str = term
        self.headers = headers
        self.artist_list: list = []
        self.artist_uri_list: list = []
        self.artist_id_list: list = []
        self.number_of_artists: int = 0
        self.top_genres: dict = None
        self.artist_tracks: TrackList = TrackList("Following", self.headers, self.aiohttp_session)
    
    # ------------------------
    # Shared Methods
    # ------------------------
    def __get_uri_list(self):
        return [artist['uri'] for artist in self.artist_list if 'uri' in artist]
    
    def __get_id_list(self):
        return [artist['id'] for artist in self.artist_list if 'id' in artist]
    
    # ------------------------
    # Followed Artists - Latest Releases
    # ------------------------
    async def get_followed_artist_latest_release(self):
        try:
            await self.artist_tracks.get_artist_latest_release(self.artist_id_list)
        except Exception as err:
            log.error(f"Get Followed Artists Latest Release: {err}")
            raise Exception(f"Get Followed Artists Latest Release: {err}") from err
        
    async def aiohttp_get_followed_artist_latest_release(self):
        try:
            await self.artist_tracks.aiohttp_get_artist_latest_release(self.artist_id_list)
        except Exception as err:
            log.error(f"AIOHTTP Get Followed Artists Latest Release: {err}")
            raise Exception(f"AIOHTTP Get Followed Artists Latest Release: {err}") from err

    # ------------------------
    # Get Followed Artists
    # ------------------------
    async def get_followed_artists(self):
        try:
            log.info("Getting followed artists...")
            artist_ids = []

            url = f"{self.BASE_URL}/me/following?type=artist&limit=50"
            
            while url:
                response = requests.get(url, headers=self.headers)
                response_data = response.json()

                if response.status_code != 200:
                    raise Exception(f"Error fetching followed artists: {response_data}")
                
                ids = [artist['id'] for artist in response_data['artists']['items']]
                artist_ids.extend(ids)
                
                # Get next page URL or None
                url = response_data['artists'].get('next')
            
            self.artist_id_list = artist_ids
            log.info(f"Found {len(artist_ids)} followed artists!")
        except Exception as err:
            log.error(f"Get Followed Artists: {err}")
            raise Exception(f"Get Followed Artists: {err}") from err
    
    async def aiohttp_get_followed_artists(self):
        try:
            log.info("Getting followed artists (aiohttp)...")
            url = f"{self.BASE_URL}/me/following?type=artist&limit=50"
            
            artist_ids = []

            while url:
                data = await fetch_json(self.aiohttp_session, url, headers=self.headers)
                ids = [artist['id'] for artist in data['artists']['items']]
                artist_ids.extend(ids)
                url = data["artists"].get("next")

            self.artist_id_list = artist_ids
            log.info(f"Found {len(artist_ids)} followed artists!")
        except Exception as err:
            log.error(f"AIOHTTP Get Followed Artists: {err}")
            raise Exception(f"AIOHTTP Get Followed Artists: {err}") from err
        
    # ------------------------
    # Set Top Artists
    # ------------------------
    async def set_top_artists(self):
        try:
            log.info(f"Setting Top Artists for term: {self.term}")
            self.artist_list = await self.get_top_artists()
            self.artist_uri_list = self.__get_uri_list()
            self.artist_id_list = self.__get_id_list()
            self.get_top_genres()
            self.number_of_artists = len(self.artist_list)
            log.info(f"Top Artists Set successfully! Count: {self.number_of_artists}")
        except Exception as err:
            log.error(f"Set User Top Artists: {err}")
            raise Exception(f"Set User Top Artists {self.term}: {err}") from err
    
    async def aiohttp_set_top_artists(self):
        try:
            log.info(f"Setting Top Artists for term: {self.term}")
            self.artist_list = await self.aiohttp_get_top_artists()
            self.artist_uri_list = self.__get_uri_list()
            self.artist_id_list = self.__get_id_list()
            self.get_top_genres()
            self.number_of_artists = len(self.artist_list)
            log.info(f"Top Artists Set successfully! Count: {self.number_of_artists}")
        except Exception as err:
            log.error(f"AIOHTTP Set User Top Artists: {err}")
            raise Exception(f"AIOHTTP Set User Top Artists {self.term}: {err}") from err
    
    # ------------------------
    # Get Top Artists
    # ------------------------
    async def get_top_artists(self):
        try:
            url = f"{self.BASE_URL}/me/top/artists?limit=25&time_range={self.term}"

            response = requests.get(url, headers=self.headers)
            response_data = response.json()

            if response.status_code != 200:
                raise Exception(f"Error fetching top artists: {response_data}")

            return response_data['items']
        except Exception as err:
            log.error(f"Get User Top Artists: {err}")
            raise Exception(f"Get User Top Artists {self.term}: {err}") from err
    
    async def aiohttp_get_top_artists(self):
        try:
            url = f"{self.BASE_URL}/me/top/artists?limit=25&time_range={self.term}"
            data = await fetch_json(self.aiohttp_session, url, headers=self.headers)
            return data['items']
        except Exception as err:
            log.error(f"AIOHTTP Get User Top Artists: {err}")
            raise Exception(f"AIOHTTP Get User Top Artists {self.term}: {err}") from err
    
    # ------------------------
    # Get Top Genres
    # ------------------------
    def get_top_genres(self):
        try:
            log.info(f"Calculating top genres for term {self.term}...")
            
            # Count genre occurrences
            genre_counts = {}
            for artist in self.artist_list:
                for genre in artist.get('genres', []):
                    genre_counts[genre] = genre_counts.get(genre, 0) + 1
            
            self.top_genres = genre_counts
            log.info(f"Found {len(genre_counts)} unique genres")
        except Exception as err:
            log.error(f"Get Top Genres: {err}")
            raise Exception(f"Get Top Genres: {err}") from err