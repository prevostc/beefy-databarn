"""BeefyDatabarn tap class."""

from __future__ import annotations

from typing import TYPE_CHECKING

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_beefy_databarn.contract_creation.contract_creation_stream import ContractCreationDateStream

if TYPE_CHECKING:
    from singer_sdk.streams import Stream


class TapBeefyDatabarn(Tap):
    """BeefyDatabarn tap class."""

    name = "tap-beefy-databarn"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "postgres_connection_string",
            th.StringType,
            required=True,
            description="Postgres connection string",
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            ContractCreationDateStream(self),
        ]


if __name__ == "__main__":
    TapBeefyDatabarn.cli()
