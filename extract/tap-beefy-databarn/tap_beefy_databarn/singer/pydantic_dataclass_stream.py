from __future__ import annotations

import typing as t
from abc import ABCMeta, abstractmethod

import jsonref  # type: ignore  # noqa: PGH003
from pydantic import BaseModel
from singer_sdk.streams import Stream

if t.TYPE_CHECKING:
    from os import PathLike

    import singer_sdk._singerlib as singer
    from singer_sdk import Tap


R = t.TypeVar("R", bound=BaseModel)

class PydanticDataclassStream(Stream, t.Generic[R], metaclass=ABCMeta):
    """A stream that uses a pydantic dataclass to generate it's schema."""

    record_dataclass: type[R]

    def __init__(self, tap: Tap, schema: str | PathLike | dict[str, t.Any] | singer.Schema | None = None, name: str | None = None) -> None:
        if schema is not None:
            msg = "schema must be None"
            raise Exception(msg)

        if self.record_dataclass is None:
            msg = "record_dataclass must be set"
            raise Exception(msg)

        schema = self.record_dataclass.model_json_schema()
        schema = jsonref.replace_refs(schema, jsonschema=True, lazy_load=False)
        schema = t.cast(dict[str, t.Any], schema)  # we can cast because lazy_load=False
        if "$defs" in schema:
            del schema["$defs"]

        super().__init__(tap, schema, name)

        self.logger.info("schema: %s", schema)

    @abstractmethod
    def get_models(self, context: dict[t.Any, t.Any] | None) -> t.Iterable[R]:
        """
        Return an iterable of pydantic dataclasses.
        """
        msg = "get_models must be implemented"
        raise NotImplementedError(msg)

    def get_records(self, context: dict[t.Any, t.Any] | None) -> t.Iterable[dict]:
        """
        Return an iterable of dicts.
        """
        for obj in self.get_models(context):
            yield obj.model_dump()
