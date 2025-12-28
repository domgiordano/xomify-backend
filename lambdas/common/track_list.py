import requests
import aiohttp
import time
from datetime import datetime, timedelta, date
import asyncio
from lambdas.common.aiohttp_helper import fetch_json
from lambdas.common.logger import get_logger

log = get_logger(__file__)

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
        
        # Custom date window (set by cron for specific week processing)
        # If None, uses dynamic "current week" calculation
        self.week_start: datetime = None
        self.week_end: datetime = None
    
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
            
            # Log the date window being used
            if self.week_start and self.week_end:
                log.info(f"Using custom date window: {self.week_start.strftime('%Y-%m-%d')} to {self.week_end.strftime('%Y-%m-%d')}")
            else:
                log.info("Using dynamic current week calculation")
            
            # Process in batches to avoid overwhelming the API
            batch_size = 20
            all_release_uris = []
            artists_with_releases = 0
            
            for i in range(0, len(artist_id_list), batch_size):
                batch = artist_id_list[i:i + batch_size]
                tasks = [self.aiohttp_get_latest_releases(artist_id) for artist_id in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for idx, result in enumerate(batch_results):
                    if isinstance(result, Exception):
                        log.warning(f"Failed to get releases for artist {batch[idx]}: {result}")
                    elif result:  # Non-empty list
                        artists_with_releases += 1
                        all_release_uris.extend(result)
                
                # Small delay between batches to be nice to the API
                if i + batch_size < len(artist_id_list):
                    await asyncio.sleep(0.5)
            
            log.info(f"Found {len(all_release_uris)} new release URIs from {artists_with_releases} artists")

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
        Get all releases from an artist within the release window.
        
        Makes separate API calls for each include_group type because Spotify
        sorts by release_date WITHIN each group, not across all groups.
        """
        try:
            release_uris = []
            include_groups = ["album", "single", "appears_on"]
            
            for group in include_groups:
                url = f"{self.BASE_URL}/artists/{artist_id}/albums?include_groups={group}&limit=5"

                response = requests.get(url, headers=self.headers)
                response_data = response.json()

                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 1))
                    log.warning(f"Rate limit reached. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after + 1)
                    return await self.get_latest_releases(artist_id)
                
                if response.status_code != 200:
                    log.warning(f"Error fetching {group} for artist {artist_id}: {response_data}")
                    continue
                
                for release in response_data.get('items', []):
                    release_date = release.get('release_date', '')
                    if self.__is_within_release_week(release_date):
                        log.debug(f"New {group}: {release['name']} ({release_date})")
                        release_uris.append(release['uri'])
            
            return release_uris

        except Exception as err:
            log.error(f"Get Latest Releases for artist {artist_id}: {err}")
            return []  # Return empty list instead of raising to not break other artists
    
    async def aiohttp_get_latest_releases(self, artist_id: str):
        """
        Get all releases from an artist within the release window.
        
        Makes separate API calls for each include_group type because Spotify
        sorts by release_date WITHIN each group, not across all groups.
        With a single call using include_groups=album,single, you'd get
        albums first (sorted by date) then singles (sorted by date) - 
        meaning recent singles could be pushed past the limit.
        """
        try:
            release_uris = []
            
            # Query each type separately to ensure we get recent releases of each type
            # Spotify sorts by release_date within each group, not across groups
            include_groups = ["album", "single", "appears_on"]
            
            for group in include_groups:
                url = f"{self.BASE_URL}/artists/{artist_id}/albums?include_groups={group}&limit=5"
                
                try:
                    data = await fetch_json(self.aiohttp_session, url, headers=self.headers)
                    
                    for release in data.get('items', []):
                        release_date = release.get('release_date', '')
                        release_name = release.get('name', 'Unknown')
                        
                        if self.__is_within_release_week(release_date):
                            log.info(f"🎵 New {group}: {release_name} - {release_date}")
                            release_uris.append(release['uri'])
                except Exception as group_err:
                    log.warning(f"Failed to get {group} releases for artist {artist_id}: {group_err}")
                    continue
            
            return release_uris
        except Exception as err:
            log.error(f"AIOHTTP Get Latest Releases for artist {artist_id}: {err}")
            return []  # Return empty instead of raising to not break the whole batch
    
    # ------------------------
    # Get Tracks from Album
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
                    for track in album["tracks"].get("items", []):
                        track_uris.append(track["uri"])

            return track_uris
        except Exception as err:
            log.error(f"Get Several Albums Tracks: {err}")
            raise Exception(f"Get Several Albums Tracks: {err}") from err
    
    async def aiohttp_get_several_albums_tracks(self):
        """
        Batch fetch album details and extract all tracks.
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
    def __is_within_release_week(self, target_date_str: str) -> bool:
        """
        Check if a release date falls within our release window.
        
        If custom week_start/week_end are set (by cron), use those.
        Otherwise, calculate the current Sunday-Saturday week dynamically.
        
        Week Definition: Sunday 00:00:00 to Saturday 23:59:59
        """
        try:
            if not target_date_str or len(target_date_str) < 4:
                return False

            # Use custom date window if set, otherwise calculate current week
            if self.week_start and self.week_end:
                start_date = self.week_start.date() if hasattr(self.week_start, 'date') else self.week_start
                end_date = self.week_end.date() if hasattr(self.week_end, 'date') else self.week_end
            else:
                # Calculate current Sunday-Saturday week
                today = datetime.today().date()
                
                # Find last Sunday (start of current week)
                # weekday(): Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
                days_since_sunday = (today.weekday() + 1) % 7  # Sun=0, Mon=1, ..., Sat=6
                start_date = today - timedelta(days=days_since_sunday)
                
                # End date is Saturday (6 days after Sunday)
                end_date = start_date + timedelta(days=6)
            
            # Parse release date based on format
            if len(target_date_str) == 4:
                # Year only (e.g., "2024") - can't determine week, skip
                return False
            elif len(target_date_str) == 7:
                # Year-month only (e.g., "2024-12") - treat as first of month
                target_date = datetime.strptime(target_date_str, "%Y-%m").date()
            else:
                # Full date (e.g., "2024-12-19")
                target_date = datetime.strptime(target_date_str[:10], "%Y-%m-%d").date()

            # Window: start_date <= target_date <= end_date
            is_in_window = start_date <= target_date <= end_date
            
            if is_in_window:
                log.debug(f"✅ Release {target_date_str} is in window [{start_date} - {end_date}]")
            
            return is_in_window
                
        except Exception as err:
            log.warning(f"Error parsing date '{target_date_str}': {err}")
            return False

    def __split_spotify_uris(self, uris: list) -> tuple:
        """Split URIs into tracks and albums."""
        tracks = [uri for uri in uris if uri and uri.startswith("spotify:track:")]
        albums = [uri for uri in uris if uri and uri.startswith("spotify:album:")]
        return tracks, albums
