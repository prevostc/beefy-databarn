import typing as t

from urllib3 import Timeout

if t.TYPE_CHECKING:
    from requests.adapters import HTTPAdapter
    MIXIN_BASE = HTTPAdapter
else:
    MIXIN_BASE = object

class TimeoutMixin(MIXIN_BASE):
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN003, ANN002
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        else:
            self.timeout = Timeout(connect=5, read=10)
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):  # noqa: ANN201, ANN003, ANN001
        kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)
