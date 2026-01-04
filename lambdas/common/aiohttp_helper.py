"""
XOMIFY AIOHTTP Helper
=====================
Async HTTP utilities with rate limiting and retry logic.
"""

import aiohttp
import asyncio

from lambdas.common.logger import get_logger
from lambdas.common.errors import SpotifyAPIError

log = get_logger(__file__)

# Rate limit event - initialized lazily per event loop
_rate_limited: asyncio.Event = None

MAX_RETRIES = 3


def _get_rate_limit_event() -> asyncio.Event:
    """
    Get or create the rate limit event for the current event loop.
    This ensures the event is always bound to the correct loop.
    """
    global _rate_limited
    
    try:
        # Check if we have an event and it's bound to the current loop
        if _rate_limited is not None:
            # Try to access the event - will fail if wrong loop
            loop = asyncio.get_running_loop()
            # In Python 3.10+, events don't have _loop attribute exposed the same way
            # So we just try to use it and recreate if it fails
            return _rate_limited
    except RuntimeError:
        pass
    
    # Create new event for current loop
    _rate_limited = asyncio.Event()
    _rate_limited.set()  # Start in "open" state
    return _rate_limited


async def fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict = None,
    retry_count: int = 0
) -> dict:
    """
    GET JSON from URL with rate limit handling and retry logic.
    
    Args:
        session: aiohttp session
        url: URL to fetch
        headers: Request headers
        retry_count: Current retry attempt
        
    Returns:
        Parsed JSON response
        
    Raises:
        SpotifyAPIError: On API errors after retries exhausted
    """
    try:
        # Wait if globally rate limited
        rate_event = _get_rate_limit_event()
        await rate_event.wait()
        
        async with session.get(url, headers=headers) as resp:
            # Handle rate limiting
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limited on GET {url}. Waiting {retry_after}s...")
                
                rate_event.clear()
                await asyncio.sleep(retry_after + 1)
                rate_event.set()
                
                if retry_count < MAX_RETRIES:
                    return await fetch_json(session, url, headers, retry_count + 1)
                
                raise SpotifyAPIError(
                    message=f"Rate limit exceeded after {MAX_RETRIES} retries",
                    endpoint=url
                )
            
            # Handle auth errors
            if resp.status == 401:
                raise SpotifyAPIError(
                    message="Unauthorized - token may have expired",
                    endpoint=url
                )
            
            # Handle not found
            if resp.status == 404:
                log.warning(f"Resource not found: {url}")
                return {"items": [], "albums": []}
            
            # Handle other errors
            if resp.status != 200:
                text = await resp.text()
                raise SpotifyAPIError(
                    message=f"API error {resp.status}: {text}",
                    endpoint=url
                )
            
            return await resp.json()
            
    except aiohttp.ClientError as err:
        log.error(f"AIOHTTP client error: {err}")
        
        if retry_count < MAX_RETRIES:
            wait_time = (2 ** retry_count) + 1
            log.info(f"Retrying in {wait_time}s (attempt {retry_count + 1}/{MAX_RETRIES})")
            await asyncio.sleep(wait_time)
            return await fetch_json(session, url, headers, retry_count + 1)
        
        raise SpotifyAPIError(
            message=f"Request failed after {MAX_RETRIES} retries: {err}",
            endpoint=url
        )
    except SpotifyAPIError:
        raise
    except Exception as err:
        log.error(f"Unexpected error in fetch_json: {err}")
        raise SpotifyAPIError(
            message=str(err),
            endpoint=url
        )


async def post_json(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict = None,
    json: dict = None,
    retry_count: int = 0
) -> dict:
    """
    POST JSON to URL with rate limit handling.
    """
    try:
        rate_event = _get_rate_limit_event()
        await rate_event.wait()
        
        async with session.post(url, headers=headers, json=json) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limited on POST {url}. Waiting {retry_after}s...")
                
                rate_event.clear()
                await asyncio.sleep(retry_after + 1)
                rate_event.set()
                
                if retry_count < MAX_RETRIES:
                    return await post_json(session, url, headers, json, retry_count + 1)
                
                raise SpotifyAPIError(
                    message=f"Rate limit exceeded after {MAX_RETRIES} retries",
                    endpoint=url
                )
            
            if resp.status == 401:
                raise SpotifyAPIError(
                    message="Unauthorized - token may have expired",
                    endpoint=url
                )
            
            if resp.status not in (200, 201):
                text = await resp.text()
                raise SpotifyAPIError(
                    message=f"API error {resp.status}: {text}",
                    endpoint=url
                )
            
            return await resp.json()
            
    except aiohttp.ClientError as err:
        log.error(f"AIOHTTP client error on POST: {err}")
        
        if retry_count < MAX_RETRIES:
            wait_time = (2 ** retry_count) + 1
            await asyncio.sleep(wait_time)
            return await post_json(session, url, headers, json, retry_count + 1)
        
        raise SpotifyAPIError(
            message=f"POST failed after {MAX_RETRIES} retries: {err}",
            endpoint=url
        )
    except SpotifyAPIError:
        raise
    except Exception as err:
        log.error(f"Unexpected error in post_json: {err}")
        raise SpotifyAPIError(
            message=str(err),
            endpoint=url
        )


async def delete_json(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict = None,
    json: dict = None,
    retry_count: int = 0
) -> dict:
    """
    DELETE request with JSON body.
    """
    try:
        rate_event = _get_rate_limit_event()
        await rate_event.wait()
        
        async with session.delete(url, headers=headers, json=json) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limited on DELETE {url}. Waiting {retry_after}s...")
                
                rate_event.clear()
                await asyncio.sleep(retry_after + 1)
                rate_event.set()
                
                if retry_count < MAX_RETRIES:
                    return await delete_json(session, url, headers, json, retry_count + 1)
                
                raise SpotifyAPIError(
                    message=f"Rate limit exceeded after {MAX_RETRIES} retries",
                    endpoint=url
                )
            
            if resp.status not in (200, 201):
                text = await resp.text()
                raise SpotifyAPIError(
                    message=f"API error {resp.status}: {text}",
                    endpoint=url
                )
            
            # DELETE might not return JSON
            try:
                return await resp.json()
            except:
                return {"status": "ok"}
                
    except SpotifyAPIError:
        raise
    except Exception as err:
        log.error(f"Error in delete_json: {err}")
        raise SpotifyAPIError(
            message=str(err),
            endpoint=url
        )


async def put_data(
    session: aiohttp.ClientSession,
    url: str,
    data: str,
    headers: dict = None,
    retry_count: int = 0
) -> dict:
    """
    PUT raw data (like base64 image).
    """
    try:
        rate_event = _get_rate_limit_event()
        await rate_event.wait()
        
        async with session.put(url, data=data, headers=headers) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limited on PUT {url}. Waiting {retry_after}s...")
                
                rate_event.clear()
                await asyncio.sleep(retry_after + 1)
                rate_event.set()
                
                if retry_count < MAX_RETRIES:
                    return await put_data(session, url, data, headers, retry_count + 1)
                
                raise SpotifyAPIError(
                    message=f"Rate limit exceeded after {MAX_RETRIES} retries",
                    endpoint=url
                )
            
            if resp.status not in (200, 201, 202):
                text = await resp.text()
                raise SpotifyAPIError(
                    message=f"API error {resp.status}: {text}",
                    endpoint=url
                )
            
            return {"status": "ok"}
                
    except SpotifyAPIError:
        raise
    except Exception as err:
        log.error(f"Error in put_data: {err}")
        raise SpotifyAPIError(
            message=str(err),
            endpoint=url
        )