import typing as t

from pyrate_limiter import Duration, Limiter, RequestRate
from requests_cache import CacheMixin, RedisCache
from requests_ratelimiter import LimiterMixin, RedisBucket

if t.TYPE_CHECKING:
    from requests.adapters import HTTPAdapter
    MIXIN_BASE = HTTPAdapter
else:
    MIXIN_BASE = object



class RedisBucketRateLimitMixin(CacheMixin, LimiterMixin, MIXIN_BASE): # type: ignore  # noqa: PGH003
    """Session class with caching and rate-limiting behavior. Accepts arguments for both
    LimiterSession and CachedSession.
    """
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN003, ANN002
        kwargs["bucket_class"] = kwargs.get("bucket_class", RedisBucket("redis://localhost:6379/0"))
        kwargs["backend"] = kwargs.get("backend", RedisCache("redis://localhost:6379/0"))

        super().__init__(*args, **kwargs)


class InMemoryBucketRateLimitMixin(LimiterMixin, MIXIN_BASE): # type: ignore  # noqa: PGH003
    """
    Session class with rate-limiting behavior. Accepts arguments for LimiterSession.
    """
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN003, ANN002

        # requests_ratelimiter aims to fill the RPS as much as possible, but we want to
        # ensure we have a minimum delay between requests.
        if "min_seconds_between_requests" in kwargs:
            min_seconds_between_requests = kwargs.pop("min_seconds_between_requests")
            assert isinstance(min_seconds_between_requests, int), (
                "min_seconds_between_requests must be an integer"
            )
            assert "limiter" not in kwargs, "limiter cannot be specified when using min_seconds_between_requests"
            assert "per_second" not in kwargs, "per_second cannot be specified when using min_seconds_between_requests"
            assert "per_minute" not in kwargs, "per_minute cannot be specified when using min_seconds_between_requests"
            assert "per_hour" not in kwargs, "per_hour cannot be specified when using min_seconds_between_requests"
            assert "per_day" not in kwargs, "per_day cannot be specified when using min_seconds_between_requests"
            assert "per_month" not in kwargs, "per_month cannot be specified when using min_seconds_between_requests"

            custom_rate = RequestRate(1, Duration.SECOND * min_seconds_between_requests)
            limiter = Limiter(custom_rate)

            kwargs["limiter"] = kwargs.get("limiter", limiter)

        super().__init__(*args, **kwargs)
