import asyncio
import typing as t

T = t.TypeVar("T")
async def rate_limit_iterator(iterator: t.Iterable[T], max_rps: float) -> t.AsyncIterable[T]:
    """Rate limit an async iterable."""

    last_call_at = 0.0

    for item in iterator:
        now = asyncio.get_running_loop().time()
        if now - last_call_at < 1 / max_rps:
            await asyncio.sleep(1 / max_rps - (now - last_call_at))
        last_call_at = asyncio.get_running_loop().time()
        yield item
