import os
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")
V = TypeVar("V")

class EnvParser:

    def __init__(self) -> None:
        self.list_delimiter = ","

    def get(self, name: str, to_type: Callable[[str], T]) -> T | None:
        value = os.getenv(name, None)
        if not value:
            return None
        return self._cast(value, to_type)

    def require(self, name: str, to_type: Callable[[str], T]) -> T:
        value = os.getenv(name, None)
        if not value:
            msg = f"Missing {name} environment variable."
            raise ValueError(msg)
        return self._cast(value, to_type)

    def get_list(self, name: str, to_type: Callable[[str], T]) -> list[T] | None:
        value = os.getenv(name, None)
        if not value:
            return None
        return [self._cast(v, to_type) for v in value.strip().split(self.list_delimiter)]

    def require_list(self, name: str, to_type: Callable[[str], T]) -> list[T]:
        value = os.getenv(name, None)
        if not value:
            msg = f"Missing {name} environment variable."
            raise ValueError(msg)
        return [self._cast(v, to_type) for v in value.strip().split(self.list_delimiter)]

    def _cast(self, value: str, to_type: Callable[[str], T]) -> T:
        if to_type == bool:
            return value.lower() in ["1", "true"] # type: ignore  # noqa: PGH003
        return to_type(value.strip())
