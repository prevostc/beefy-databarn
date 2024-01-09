"""Stream type classes for tap-beefy-databarn."""

from __future__ import annotations

import json
import typing as t
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg.rows import class_row
from singer_sdk.streams import Stream
import singer_sdk._singerlib as singer

from tap_beefy_databarn.common.chains import all_chains

if t.TYPE_CHECKING:
    from tap_beefy_databarn.common.chains import ChainType

EventType = t.Literal[
    "IERC20:Transfer",
    "BeefyZapRouter:FulfilledOrder",
]


@dataclass
class ContractEventWatch:
    chain: ChainType
    contract_address: str
    events: list[EventType]


@dataclass
class SquidImportState:
    chain: ChainType
    query_height: int
    contract_addresses: set[str]


class SquidContractEventsStream(Stream):
    """Fetches the contract creation date for a given contract address."""

    name = "contract_event_data"
    primary_keys: t.ClassVar[list[str]] = ["chain", "contract_address", "transaction_hash", "block_number", "log_index"]
    schema_filepath = Path(__file__).parent / "./contract_event_data.json"
    replication_key = "query_height"
    is_sorted = False

    @property
    def schema(self) -> dict:
        return json.loads(Path(self.schema_filepath).read_text())

    @property
    def partitions(self) -> list[dict] | None:
        return [{"chain": c, "height": 0} for c in all_chains]

    def _generate_schema_messages(
        self,
    ) -> t.Generator[singer.SchemaMessage, None, None]:
        """Generate schema messages from stream maps.

        Yields:
            Schema message objects.
        """
        bookmark_keys = [self.replication_key] if self.replication_key else None
        for stream_map in self.stream_maps:
            if isinstance(stream_map, RemoveRecordTransform):
                # Don't emit schema if the stream's records are all ignored.
                continue

            yield singer.SchemaMessage(
                stream_map.stream_alias,
                stream_map.transformed_schema,
                stream_map.transformed_key_properties,
                bookmark_keys,
            )

    def get_records(self, context: dict[t.Any, t.Any] | None) -> t.Iterable[dict]:  # noqa: ARG002
        self.logger.info("Fetching contract creation date: %s", self.schema)

        by_chain: dict[ChainType, list[ContractEventWatch]] = {}
        for contract in self._get_contract_list():
            by_chain.setdefault(contract.chain, []).append(contract)

        for chain, contracts in by_chain.items():
            yield from self._get_records_for_chain(chain, contracts)

    def _get_watch_list(self) -> t.Iterable[ContractEventWatch]:
        """Get the list of contracts to watch for events."""
        connection_string = self.config.get("postgres_connection_string", "")
        with psycopg.Connection.connect(connection_string) as conn, conn.cursor(row_factory=class_row(ContractEventWatch)) as cur:
            cur.execute(
                """
                SELECT
                    chain,
                    contract_address,
                    events
                FROM analytics.event_indexer_watchlist
            """,
            )
            return cur.fetchall()

    # def _get_records_for_chain(self, contracts: list[ContractEventWatch]):
    #     assert 0 <= first_block <= last_block
    #     query = dict(query)  # copy query to mess with it later

    #     archived_height = int(get_text(f"{archive_url}/height"))
    #     next_block = first_block
    #     last_block = min(last_block, archived_height)

    #     while next_block <= last_block:
    #         worker_url = get_text(f"{archive_url}/{next_block}/worker")

    #         query["fromBlock"] = next_block
    #         query["toBlock"] = last_block
    #         res = requests.post(worker_url, json=query)
    #         res.raise_for_status()
    #         blocks = res.json()

    #         last_processed_block = blocks[-1]["header"]["number"]
    #         next_block = last_processed_block + 1
    #         for block in blocks:
    #             print(json.dumps(block))
