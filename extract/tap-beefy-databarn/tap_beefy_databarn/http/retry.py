import typing as t

from urllib3.util import Retry

if t.TYPE_CHECKING:
    from requests.adapters import HTTPAdapter
    MIXIN_BASE = HTTPAdapter
else:
    MIXIN_BASE = object

class RetryMixin(MIXIN_BASE):
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN003, ANN002
        kwargs["max_retries"] = kwargs.get("max_retries", Retry(total=3, backoff_factor=0.2))
        super().__init__(*args, **kwargs)

    # TODO: retry on requests.exceptions.ReadTimeout
    # TODO: retry when the user determines it's appropriate
