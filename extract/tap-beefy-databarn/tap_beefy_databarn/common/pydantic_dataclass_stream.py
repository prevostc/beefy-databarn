from __future__ import annotations

import typing as t
from os import PathLike

import singer_sdk._singerlib as singer
from pydantic import RootModel, TypeAdapter
from singer_sdk import Tap
from singer_sdk.streams import Stream
import jsonref


class PydanticDataclassStream(Stream):
    """A stream that uses a pydantic dataclass to generate it's schema."""

    record_dataclass: t.ClassVar | None = None

    def __init__(self, tap: Tap, schema: str | PathLike | dict[str, t.Any] | singer.Schema | None = None, name: str | None = None) -> None:
        if schema is not None:
            msg = "schema must be None"
            raise Exception(msg)

        if self.record_dataclass is None:
            msg = "record_dataclass must be set"
            raise Exception(msg)

        schema = TypeAdapter(self.record_dataclass).json_schema()
        schema = jsonref.replace_refs(schema, jsonschema=True)

        super().__init__(tap, schema, name)

        self.logger.info("schema: %s", schema)

    def _pydantic_dataclass_to_dict(self, obj: t.Any) -> dict[str, t.Any]:
        return RootModel[self.record_dataclass](obj).model_dump()
