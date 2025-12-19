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

# Global rate limit event - blocks all requests when rate limited
_rate_limited = asyncio.Event()
_rate_limited.set()  # Start in "open" state

MAX_RETRIES = 3


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
        await _rate_limited.wait()
        
        async with session.get(url, headers=headers) as resp:
            # Handle rate limiting
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limited on GET {url}. Waiting {retry_after}s...")
                
                _rate_limited.clear()
                await asyncio.sleep(retry_after + 1)
                _rate_limited.set()
                
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
        await _rate_limited.wait()
        
        async with session.post(url, headers=headers, json=json) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limited on POST {url}. Waiting {retry_after}s...")
                
                _rate_limited.clear()
                await asyncio.sleep(retry_after + 1)
                _rate_limited.set()
                
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
        await _rate_limited.wait()
        
        async with session.delete(url, headers=headers, json=json) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limited on DELETE {url}. Waiting {retry_after}s...")
                
                _rate_limited.clear()
                await asyncio.sleep(retry_after + 1)
                _rate_limited.set()
                
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
        await _rate_limited.wait()
        
        async with session.put(url, data=data, headers=headers) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 1))
                log.warning(f"Rate limited on PUT {url}. Waiting {retry_after}s...")
                
                _rate_limited.clear()
                await asyncio.sleep(retry_after + 1)
                _rate_limited.set()
                
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
