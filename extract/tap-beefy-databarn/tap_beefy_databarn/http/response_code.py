import typing as t

if t.TYPE_CHECKING:
    from requests.adapters import HTTPAdapter
    MIXIN_BASE = HTTPAdapter
else:
    MIXIN_BASE = object

class ResponseCodeMixin(MIXIN_BASE):

    expect_response_codes: list[int] = [200, 301, 302]  # noqa: RUF012

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN003, ANN002
        if "expected_response_codes" in kwargs:
            self.expect_response_codes = kwargs["expected_response_codes"]
            del kwargs["expected_response_codes"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):  # noqa: ANN201, ANN003, ANN001
        response = super().send(request, **kwargs)

        if response.status_code not in self.expect_response_codes:
            msg = f"Error from api: {response.status_code}. Expected {self.expect_response_codes}"
            raise Exception(msg)

        return response
