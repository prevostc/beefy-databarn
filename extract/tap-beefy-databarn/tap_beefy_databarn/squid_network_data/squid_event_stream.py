"""Stream type classes for tap-beefy-databarn."""

from __future__ import annotations

import datetime
import typing as t

import psycopg
from psycopg.rows import class_row
from pydantic.dataclasses import dataclass

from tap_beefy_databarn.common.chains import ChainType, all_chains
from tap_beefy_databarn.common.events import AnyEvent
from tap_beefy_databarn.common.pydantic_dataclass_stream import PydanticDataclassStream

EventType = t.Literal[
    "IERC20:Transfer",
    "BeefyZapRouter:FulfilledOrder",
]


@dataclass
class ContractEventWatch:
    chain: ChainType
    contract_address: str
    events: list[EventType]
    creation_block_number: int
    creation_block_datetime: datetime.datetime


@dataclass
class SquidImportState:
    chain: ChainType
    last_seen_height: int
    watched_contract: list[ContractEventWatch]


@dataclass
class SquidEventStreamRecord:
    unique_key: str
    chain: ChainType
    last_seen_height: int
    # watched_contract: list[ContractEventWatch]
    event: AnyEvent | None  # None when we just want to update the last_seen_height


class SquidContractEventsStream(PydanticDataclassStream):
    """Fetches the contract creation date for a given contract address."""

    name = "squid_event_stream"
    record_dataclass = SquidEventStreamRecord
    replication_key = "last_seen_height"
    primary_keys: t.ClassVar[list[str]] = ["unique_key"]
    is_sorted = False

    @property
    def partitions(self) -> list[dict] | None:
        # return [{"chain": c, "height": 0} for c in all_chains]
        return [{"chain": c, "height": 0} for c in all_chains[0:1]]

    def get_records(self, context: dict[t.Any, t.Any] | None) -> t.Iterable[dict]:  # noqa: ARG002
        self.logger.info("Fetching squid events")

        by_chain: dict[ChainType, list[ContractEventWatch]] = {}
        for contract in self._get_watch_list():
            by_chain.setdefault(contract.chain, []).append(contract)

        for chain, contracts in by_chain.items():
            for event in self._get_records_for_chain(chain, contracts):
                yield self._pydantic_dataclass_to_dict(event)

    def _get_watch_list(self) -> t.Iterable[ContractEventWatch]:
        """Get the list of contracts to watch for events."""
        connection_string = self.config.get("postgres_connection_string", "")
        with psycopg.Connection.connect(connection_string) as conn, conn.cursor(row_factory=class_row(ContractEventWatch)) as cur:
            cur.execute(
                """
                SELECT
                    chain,
                    contract_address,
                    events,
                    creation_block_number,
                    creation_block_datetime
                FROM analytics.event_indexer_watchlist
            """,
            )
            return cur.fetchall()

    def _get_records_for_chain(self, chain: ChainType, contracts: list[ContractEventWatch]):
        """Get the records for a given chain."""
        block_number = 12
        log_index = 0
        unique_key = f"{chain}-{block_number}-{log_index}"
        yield SquidEventStreamRecord(unique_key=unique_key, chain=chain, last_seen_height=0, watched_contract=contracts, event=None)
        yield SquidEventStreamRecord(unique_key=unique_key, chain=chain, last_seen_height=0, watched_contract=contracts, event=None)

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
