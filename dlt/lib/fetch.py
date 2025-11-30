from typing import Any, AsyncIterator, Dict, Optional, Tuple
import httpx


async def fetch_url_text(url: str) -> str:
    limits = httpx.Limits(max_keepalive_connections=100, max_connections=200)
    async with httpx.AsyncClient(limits=limits, timeout=5.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.text

async def _fetch_url_json(url: str) -> Tuple[Any, Optional[str]]:
    """Fetch a URL and return the JSON payload and its ETag, if any."""
    limits = httpx.Limits(max_keepalive_connections=100, max_connections=200)
    async with httpx.AsyncClient(limits=limits, timeout=5.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        payload = response.json()
        etag = response.headers.get("etag")
        return payload, etag

async def fetch_url_json_list(url: str) -> AsyncIterator[Dict[str, Any]]:
    payload, _ = await _fetch_url_json(url)
    if not isinstance(payload, list):
        raise ValueError("Unexpected Beefy API payload; expected a list.")

    for item in payload:
        yield item

async def fetch_url_json_dict(url: str) -> Tuple[Dict[str, Any], Optional[str]]:
    payload, etag = await _fetch_url_json(url)
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Beefy API payload; expected a dict.")
    return payload, etag
    