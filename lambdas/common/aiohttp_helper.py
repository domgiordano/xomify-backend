import aiohttp
import asyncio
from lambdas.common.constants import LOGGER

log = LOGGER.get_logger(__file__)

# Global rate limit event
rate_limited = asyncio.Event()
rate_limited.set()  # start in "open" state

# Track retry attempts for exponential backoff
MAX_RETRIES = 3

async def fetch_json(session: aiohttp.ClientSession, url: str, headers: dict = None, retry_count: int = 0):
    """
    Fetch JSON from URL with rate limit handling and exponential backoff.
    """
    try:
        await rate_limited.wait()  # wait if rate limited globally
        
        async with session.get(url, headers=headers) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limit hit on GET {url}. Waiting {retry_after}s...")

                # Block all requests globally
                rate_limited.clear()
                await asyncio.sleep(retry_after + 1)
                rate_limited.set()
                
                # Retry with incremented count
                if retry_count < MAX_RETRIES:
                    return await fetch_json(session, url, headers, retry_count + 1)
                else:
                    raise Exception(f"Max retries exceeded for {url}")

            if resp.status == 401:
                raise Exception(f"Unauthorized - token may have expired: {url}")
            
            if resp.status == 404:
                log.warning(f"Resource not found: {url}")
                return {"items": [], "albums": []}  # Return empty for not found
                
            if resp.status != 200:
                text = await resp.text()
                raise Exception(f"Spotify API error {resp.status} at {url}: {text}")

            return await resp.json()
            
    except aiohttp.ClientError as err:
        log.error(f"AIOHTTP Client Error: {err}")
        if retry_count < MAX_RETRIES:
            # Exponential backoff
            wait_time = (2 ** retry_count) + 1
            log.info(f"Retrying in {wait_time}s (attempt {retry_count + 1}/{MAX_RETRIES})")
            await asyncio.sleep(wait_time)
            return await fetch_json(session, url, headers, retry_count + 1)
        raise Exception(f"AIOHTTP Fetch JSON failed after {MAX_RETRIES} retries: {err}") from err
    except Exception as err:
        log.error(f"AIOHTTP Fetch JSON: {err}")
        raise


async def post_json(session: aiohttp.ClientSession, url: str, headers: dict = None, json: dict = None, retry_count: int = 0):
    """
    POST JSON to URL with rate limit handling and exponential backoff.
    """
    try:
        await rate_limited.wait()  # wait if rate limited globally
        
        async with session.post(url, headers=headers, json=json) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limit hit on POST {url}. Waiting {retry_after}s...")
                
                rate_limited.clear()
                await asyncio.sleep(retry_after + 1)
                rate_limited.set()
                
                if retry_count < MAX_RETRIES:
                    return await post_json(session, url, headers, json, retry_count + 1)
                else:
                    raise Exception(f"Max retries exceeded for POST {url}")

            if resp.status == 401:
                raise Exception(f"Unauthorized - token may have expired: {url}")

            if resp.status not in (200, 201):
                text = await resp.text()
                raise Exception(f"Spotify API error {resp.status} at {url}: {text}")

            return await resp.json()
            
    except aiohttp.ClientError as err:
        log.error(f"AIOHTTP Client Error on POST: {err}")
        if retry_count < MAX_RETRIES:
            wait_time = (2 ** retry_count) + 1
            log.info(f"Retrying POST in {wait_time}s (attempt {retry_count + 1}/{MAX_RETRIES})")
            await asyncio.sleep(wait_time)
            return await post_json(session, url, headers, json, retry_count + 1)
        raise Exception(f"AIOHTTP Post JSON failed after {MAX_RETRIES} retries: {err}") from err
    except Exception as err:
        log.error(f"AIOHTTP Post JSON: {err}")
        raise


async def delete_json(session: aiohttp.ClientSession, url: str, headers: dict = None, json: dict = None, retry_count: int = 0):
    """
    DELETE request with JSON body, with rate limit handling.
    """
    try:
        await rate_limited.wait()
        
        async with session.delete(url, headers=headers, json=json) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limit hit on DELETE {url}. Waiting {retry_after}s...")
                
                rate_limited.clear()
                await asyncio.sleep(retry_after + 1)
                rate_limited.set()
                
                if retry_count < MAX_RETRIES:
                    return await delete_json(session, url, headers, json, retry_count + 1)
                else:
                    raise Exception(f"Max retries exceeded for DELETE {url}")

            if resp.status not in (200, 201):
                text = await resp.text()
                raise Exception(f"Spotify API error {resp.status} at DELETE {url}: {text}")

            # DELETE might not return JSON
            try:
                return await resp.json()
            except:
                return {"status": "ok"}
                
    except Exception as err:
        log.error(f"AIOHTTP Delete JSON: {err}")
        raise


async def put_data(session: aiohttp.ClientSession, url: str, data: str, headers: dict = None, retry_count: int = 0):
    """
    PUT raw data (like base64 image) with rate limit handling.
    """
    try:
        await rate_limited.wait()
        
        async with session.put(url, data=data, headers=headers) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limit hit on PUT {url}. Waiting {retry_after}s...")
                
                rate_limited.clear()
                await asyncio.sleep(retry_after + 1)
                rate_limited.set()
                
                if retry_count < MAX_RETRIES:
                    return await put_data(session, url, data, headers, retry_count + 1)
                else:
                    raise Exception(f"Max retries exceeded for PUT {url}")

            if resp.status not in (200, 201, 202):
                text = await resp.text()
                raise Exception(f"Spotify API error {resp.status} at PUT {url}: {text}")

            return {"status": "ok"}
                
    except Exception as err:
        log.error(f"AIOHTTP Put Data: {err}")
        raise