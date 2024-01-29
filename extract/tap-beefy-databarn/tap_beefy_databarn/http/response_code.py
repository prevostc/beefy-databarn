import typing as t

if t.TYPE_CHECKING:
    from requests.adapters import HTTPAdapter
    MIXIN_BASE = HTTPAdapter
else:
    MIXIN_BASE = object

class ResponseCodeMixin(MIXIN_BASE):
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN003, ANN002
        if "expect_response_code" in kwargs:
            self.expect_response_code = kwargs["expect_response_code"]
            del kwargs["expect_response_code"]
        else:
            self.expect_response_code = 200
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):  # noqa: ANN201, ANN003, ANN001
        kwargs["expect_response_code"] = self.expect_response_code
        return super().send(request, **kwargs)
