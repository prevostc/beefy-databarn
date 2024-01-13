from __future__ import annotations

import typing as t
from os import PathLike

import singer_sdk._singerlib as singer
from pydantic import TypeAdapter
from singer_sdk import Tap
from singer_sdk.streams import Stream


class PydanticDataclassStream(Stream):
    """A stream that uses a pydantic dataclass to generate it's schema."""

    record_dataclass: t.ClassVar | None = None

    def __init__(self, tap: Tap, schema: str | PathLike | dict[str, t.Any] | singer.Schema | None = None, name: str | None = None) -> None:
        if schema is not None:
            msg = "schema must be None"
            raise Exception(msg)

        schema = TypeAdapter(self.record_dataclass).json_schema()

        super().__init__(tap, schema, name)
