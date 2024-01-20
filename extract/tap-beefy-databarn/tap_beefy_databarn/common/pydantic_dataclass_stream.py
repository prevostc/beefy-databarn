from __future__ import annotations

import typing as t

import jsonref  # type: ignore  # noqa: PGH003
from singer_sdk.streams import Stream

if t.TYPE_CHECKING:
    from os import PathLike

    import singer_sdk._singerlib as singer
    from pydantic import BaseModel
    from singer_sdk import Tap


class PydanticDataclassStream(Stream):
    """A stream that uses a pydantic dataclass to generate it's schema."""

    record_dataclass: type[BaseModel]

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

    def _pydantic_dataclass_to_dict(self, obj: BaseModel) -> dict[str, t.Any]:
        return obj.model_dump()
