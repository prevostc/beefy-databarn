"""BeefyDatabarn tap class."""

from __future__ import annotations

from typing import TYPE_CHECKING

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_beefy_databarn.contract_creation.contract_creation_stream import ContractCreationDateStream
from tap_beefy_databarn.squid_network_data.squid_event_stream import SquidContractEventsStream

if TYPE_CHECKING:
    from singer_sdk.streams import Stream


POSTGRESQL_CONFIG = th.Property(
    "postgres_connection_string",
    th.StringType,
    required=True,
    description="Postgres connection string",
)


class TapBlockExplorerContractCreationInfos(Tap):
    name = "tap-block-explorer-contract-creation-infos"
    config_jsonschema = th.PropertiesList(POSTGRESQL_CONFIG).to_dict()

    def discover_streams(self) -> list[Stream]:
        return [ContractCreationDateStream(self)]


class TapSquidContractEvents(Tap):
    name = "tap-squid-contract-events"
    config_jsonschema = th.PropertiesList(POSTGRESQL_CONFIG).to_dict()

    def discover_streams(self) -> list[Stream]:
        return [SquidContractEventsStream(self)]


if __name__ == "__main__":
    msg = "Please use `poetry run tap-block-explorer-contract-creation-infos` or `poetry run tap-squid-contract-events` to run this tap."
    raise Exception(msg)
