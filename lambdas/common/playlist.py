import requests
import aiohttp
import asyncio
import time
from lambdas.common.logger import get_logger
from lambdas.common.aiohttp_helper import fetch_json, post_json, delete_json, put_data

log = get_logger(__file__)

class Playlist:

    BASE_URL = "https://api.spotify.com/v1"

    def __init__(self, user_id: str, name: str, description: str, headers: dict, session: aiohttp.ClientSession = None):
        log.info(f"Initializing Playlist '{name}' for user_id '{user_id}.")
        self.aiohttp_session = session
        self.user_id = user_id
        self.name = name
        self.description = description
        self.headers = headers
        self.uri_list = None
        self.image = None
        self.playlist = None
        self.id = None

    # ------------------------
    # Shared Methods
    # ------------------------
    def set_id(self, id: str):
        self.id = id

    # ------------------------
    # Build / Update Flows
    # ------------------------
    async def build_playlist(self, uri_list: list, image: str):
        try:
            log.info(f"Building playlist: {self.name}")
            self.uri_list = uri_list
            self.image = image
            await self.create_playlist()
            time.sleep(2)
            await self.add_playlist_image()
            time.sleep(2)
            await self.add_playlist_songs()
            log.info(f"Playlist '{self.name}' Complete!")
        except Exception as err:
            log.error(f"Build Playlist: {err}")
            raise Exception(f"Build Playlist: {err}") from err
        
    async def aiohttp_build_playlist(self, uri_list: list, image: str):
        try:
            log.info(f"Building playlist (aiohttp): {self.name} with {len(uri_list)} tracks")
            self.uri_list = uri_list
            self.image = image
            await self.aiohttp_create_playlist()
            await asyncio.sleep(2)
            await self.aiohttp_add_playlist_image()
            await asyncio.sleep(2)
            await self.aiohttp_add_playlist_songs()
            log.info(f"Playlist '{self.name}' Complete!")
        except Exception as err:
            log.error(f"AIOHTTP Build Playlist: {err}")
            raise Exception(f"AIOHTTP Build Playlist: {err}") from err
    
    async def update_playlist(self, uri_list: list):
        try:
            log.info(f"Updating playlist: {self.name}")
            self.uri_list = uri_list
            await self.delete_playlist_songs()
            time.sleep(1)
            await self.add_playlist_songs()
            log.info(f"Playlist '{self.name}' Complete!")
        except Exception as err:
            log.error(f"Update Playlist: {err}")
            raise Exception(f"Update Playlist: {err}") from err
        
    async def aiohttp_update_playlist(self, uri_list: list):
        try:
            log.info(f"Updating playlist (aiohttp): {self.name} with {len(uri_list)} tracks")
            self.uri_list = uri_list
            await self.aiohttp_delete_playlist_songs()
            await asyncio.sleep(1)
            await self.aiohttp_add_playlist_songs()
            log.info(f"Playlist '{self.name}' Updated!")
        except Exception as err:
            log.error(f"AIOHTTP Update Playlist: {err}")
            raise Exception(f"AIOHTTP Update Playlist: {err}") from err
    
    # ------------------------
    # Create Playlist
    # ------------------------
    async def create_playlist(self):
        try:
            log.info("Creating playlist..")
            url = f"{self.BASE_URL}/users/{self.user_id}/playlists"
            body = {
                "name": self.name,
                "description": self.description,
                "public": True
            }

            response = requests.post(url, json=body, headers=self.headers)

            # Check for errors
            if response.status_code != 201:
                raise Exception(f"Error creating playlist: {response.json()}")

            self.playlist = response.json()
            self.id = self.playlist['id']
            log.info(f"Playlist Creation Complete. ID: {self.id}")
        except Exception as err:
            log.error(f"Create Playlist: {err}")
            raise Exception(f"Create Playlist: {err}") from err
    
    async def aiohttp_create_playlist(self):
        try:
            log.info("Creating playlist (aiohttp)..")
            url = f"{self.BASE_URL}/users/{self.user_id}/playlists"
            body = {"name": self.name, "description": self.description, "public": True}
            data = await post_json(self.aiohttp_session, url, headers=self.headers, json=body)
            self.playlist = data
            self.id = self.playlist['id']
            log.info(f"AIOHTTP Playlist Creation Complete. ID: {self.id}")
        except Exception as err:
            log.error(f"AIOHTTP Create Playlist: {err}")
            raise Exception (f"AIOHTTP Create Playlist: {err}") from err

    # ------------------------
    # Add Playlist Songs
    # ------------------------
    async def add_playlist_songs(self):
        try:
            log.info(f"Adding {len(self.uri_list)} songs to Playlist '{self.name}'")
            batch_size = 100

            url = f"{self.BASE_URL}/playlists/{self.id}/tracks"
            for i in range(0, len(self.uri_list), batch_size):
                batch_uris = self.uri_list[i:i + batch_size]
                body = {"uris": batch_uris}

                response = requests.post(url, json=body, headers=self.headers)

                if response.status_code == 201:
                    log.debug(f"Successfully added {len(batch_uris)} tracks.")
                else:
                    raise Exception(f"Error adding songs to playlist: {response.json()}")

            log.info("Tracks Added Successfully.")
        except Exception as err:
            log.error(f"Add Playlist Songs: {err}")
            raise Exception(f"Add Playlist Songs: {err}") from err

    async def aiohttp_add_playlist_songs(self):
        try:
            if not self.uri_list or len(self.uri_list) == 0:
                log.info("No tracks to add this week. Skipping.")
                return
                
            log.info(f"Adding {len(self.uri_list)} songs to Playlist '{self.name}' (aiohttp)")
            batch_size = 100
            url = f"{self.BASE_URL}/playlists/{self.id}/tracks"
            
            for i in range(0, len(self.uri_list), batch_size):
                batch_uris = self.uri_list[i:i+batch_size]
                body = {"uris": batch_uris}
                await post_json(self.aiohttp_session, url, headers=self.headers, json=body)
                log.debug(f"AIOHTTP Added {len(batch_uris)} tracks (batch {i//batch_size + 1})")
                
            log.info(f"AIOHTTP All {len(self.uri_list)} tracks added successfully.")
        except Exception as err:
            log.error(f"AIOHTTP Add Playlist Songs: {err}")
            raise Exception (f"AIOHTTP Add Playlist Songs: {err}") from err

    # ------------------------
    # Add Playlist Image
    # ------------------------
    async def add_playlist_image(self, retried: bool = False):
        try:
            log.info(f"Adding Image to Playlist {self.id}...")
            url = f'{self.BASE_URL}/playlists/{self.id}/images'
            body = self.image.replace('\n', '')

            response = requests.put(url, body, headers=self.headers)

            if response.status_code != 202:
                if not retried:
                    log.warning("First attempt failed. Retrying...")
                    time.sleep(2)
                    await self.add_playlist_image(True)
                else:
                    raise Exception(f"Failed to upload image: {response.status_code} {response.text}")
            
            log.info(f"Image added to Playlist. Name: {self.name} ID: {self.id}.")

        except Exception as err:
            log.error(f"Adding Playlist Image: {err}")
            raise Exception(f"Adding Playlist Image: {err}")
    
    async def aiohttp_add_playlist_image(self, retried=False):
        try:
            log.info(f"Adding Image to Playlist {self.id} (aiohttp)...")
            url = f'{self.BASE_URL}/playlists/{self.id}/images'
            body = self.image.replace('\n', '')
            
            # Use the helper with retry built in
            await put_data(self.aiohttp_session, url, data=body, headers=self.headers)
            log.info("AIOHTTP Image added to Playlist.")
        except Exception as err:
            log.error(f"AIOHTTP Add Playlist Image: {err}")
            if not retried:
                log.warning("Retrying image upload...")
                await asyncio.sleep(2)
                await self.aiohttp_add_playlist_image(True)
            else:
                raise Exception(f"AIOHTTP Add Playlist Image: {err}") from err

    # ------------------------
    # Delete Playlist Songs
    # ------------------------
    async def delete_playlist_songs(self):
        try:
            log.info(f"Deleting all songs from playlist {self.id}...")
            
            # Fetch all track URIs in the playlist
            tracks_to_remove = []
            limit = 100
            offset = 0

            while True:
                url = f"{self.BASE_URL}/playlists/{self.id}/tracks?limit={limit}&offset={offset}"
                resp = requests.get(url, headers=self.headers)
                
                if resp.status_code != 200:
                    raise Exception(f"Error fetching playlist tracks: {resp.json()}")
                    
                data = resp.json()
                items = data.get("items", [])
                if not items:
                    break
                tracks_to_remove.extend([{"uri": item["track"]["uri"]} for item in items if item.get("track")])
                offset += len(items)

            log.info(f"Found {len(tracks_to_remove)} tracks to remove")

            # Delete tracks in batches of 100
            for i in range(0, len(tracks_to_remove), 100):
                batch = tracks_to_remove[i:i+100]
                payload = {"tracks": batch}
                del_url = f"{self.BASE_URL}/playlists/{self.id}/tracks"
                resp = requests.delete(del_url, headers=self.headers, json=payload)
                if resp.status_code not in (200, 201):
                    log.error(f"Error deleting batch: {resp.status_code} {resp.text}")

            log.info("Tracks removed successfully.")
        except Exception as err:
            log.error(f"Delete Playlist Songs: {err}")
            raise Exception(f"Delete Playlist Songs: {err}") from err
        
    async def aiohttp_delete_playlist_songs(self):
        try:
            log.info(f"Deleting all songs from playlist {self.name} (aiohttp)")
            
            # Fetch all tracks
            tracks_to_remove = []
            limit, offset = 100, 0
            
            while True:
                url = f"{self.BASE_URL}/playlists/{self.id}/tracks?limit={limit}&offset={offset}"
                data = await fetch_json(self.aiohttp_session, url, headers=self.headers)
                items = data.get("items", [])
                if not items:
                    break
                tracks_to_remove.extend([
                    {"uri": item["track"]["uri"]} 
                    for item in items 
                    if item.get("track") and item["track"].get("uri")
                ])
                offset += len(items)
            
            if not tracks_to_remove:
                log.info("No tracks to remove.")
                return
                
            log.info(f"Removing {len(tracks_to_remove)} tracks...")
            
            # Delete in batches
            for i in range(0, len(tracks_to_remove), 100):
                batch = tracks_to_remove[i:i+100]
                payload = {"tracks": batch}
                url = f"{self.BASE_URL}/playlists/{self.id}/tracks"
                await delete_json(self.aiohttp_session, url, headers=self.headers, json=payload)
                
            log.info("AIOHTTP Tracks removed successfully.")
        except Exception as err:
            log.error(f"AIOHTTP Delete Playlist Songs: {err}")
            raise Exception(f"AIOHTTP Delete Playlist Songs: {err}") from err