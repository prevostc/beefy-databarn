"""Caching utilities with TTL and cache headers."""
from functools import wraps
from typing import Callable, Any

from fastapi import Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from api.lib.config import settings


def cached(ttl_seconds: int = None):
    """
    Decorator to add cache headers to responses.

    Args:
        ttl_seconds: Time to live in seconds (defaults to settings.CACHE_TTL_SECONDS)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            
            # If result is already a Response, add headers to it
            if isinstance(result, Response):
                ttl = ttl_seconds or settings.CACHE_TTL_SECONDS
                result.headers["Cache-Control"] = f"public, max-age={ttl}"
                return result
            
            # Convert Pydantic models to dict for JSON serialization
            if isinstance(result, BaseModel):
                result = result.model_dump()
            
            # Otherwise, wrap in JSONResponse with cache headers
            ttl = ttl_seconds or settings.CACHE_TTL_SECONDS
            response = JSONResponse(content=result)
            response.headers["Cache-Control"] = f"public, max-age={ttl}"
            return response
        
        return wrapper
    return decorator

