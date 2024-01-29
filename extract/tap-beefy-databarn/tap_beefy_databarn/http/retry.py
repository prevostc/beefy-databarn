import typing as t

from urllib3.util import Retry

if t.TYPE_CHECKING:
    from requests.adapters import HTTPAdapter
    MIXIN_BASE = HTTPAdapter
else:
    MIXIN_BASE = object

class RetryMixin(MIXIN_BASE):
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN003, ANN002
        if "max_retries" in kwargs:
            self.timeout = kwargs["max_retries"]
            del kwargs["max_retries"]
        else:
            self.max_retries = Retry(
                total=3,
                backoff_factor=0.5,
            )
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):  # noqa: ANN201, ANN003, ANN001
        kwargs["max_retries"] = self.timeout
        return super().send(request, **kwargs)
