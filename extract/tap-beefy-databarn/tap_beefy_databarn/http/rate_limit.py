import typing as t

from requests_cache import CacheMixin, RedisCache
from requests_ratelimiter import LimiterMixin, RedisBucket

if t.TYPE_CHECKING:
    from requests.adapters import HTTPAdapter
    MIXIN_BASE = HTTPAdapter
else:
    MIXIN_BASE = object

class RedisSharedRateLimitMixin(CacheMixin, LimiterMixin, MIXIN_BASE): # type: ignore  # noqa: PGH003
    """Session class with caching and rate-limiting behavior. Accepts arguments for both
    LimiterSession and CachedSession.
    """
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN003, ANN002
        kwargs["bucket_class"] = kwargs.get("bucket_class", RedisBucket("redis://localhost:6379/0"))
        kwargs["backend"] = kwargs.get("backend", RedisCache("redis://localhost:6379/0"))

        super().__init__(*args, **kwargs)


class InMemoryRateLimitMixin(LimiterMixin, MIXIN_BASE): # type: ignore  # noqa: PGH003
    """
    Session class with rate-limiting behavior. Accepts arguments for LimiterSession.
    """
