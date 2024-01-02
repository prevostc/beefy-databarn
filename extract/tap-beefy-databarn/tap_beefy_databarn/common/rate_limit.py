import time
import typing as t
from datetime import UTC, datetime

T = t.TypeVar("T")
def rate_limit_iterator(iterator: t.Iterable[T], max_rps: float) -> t.Iterable[T]:
    """Rate limit an iterable."""

    last_call_at = None

    for item in iterator:
        if last_call_at is not None:
            time_since_last_call = datetime.now(UTC) - last_call_at
            time_to_wait = 1 / max_rps - time_since_last_call.total_seconds()
            if time_to_wait > 0:
                time.sleep(time_to_wait)
        last_call_at = datetime.now(UTC)
        yield item
