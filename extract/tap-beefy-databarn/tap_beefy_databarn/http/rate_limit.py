import typing as t

from requests_cache import CacheMixin, RedisCache
from requests_ratelimiter import LimiterMixin, RedisBucket

if t.TYPE_CHECKING:
    from requests.adapters import HTTPAdapter
    MIXIN_BASE = HTTPAdapter
else:
    MIXIN_BASE = object


# TODO: requests_ratelimiter aims to fill the RPS as much as possible, but we want to
#       ensure we have a minimum delay between requests. We could implement this by
#       overriding the Limiter.try_acquire method, but it would be nice to have a
#       more general solution.

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
