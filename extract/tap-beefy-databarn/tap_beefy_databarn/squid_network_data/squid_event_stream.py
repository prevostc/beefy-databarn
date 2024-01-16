"""Stream type classes for tap-beefy-databarn."""

from __future__ import annotations

import datetime
import typing as t

import psycopg
import requests
from psycopg.rows import class_row
from pydantic import BaseModel

from tap_beefy_databarn.common.chains import ChainType, all_chains
from tap_beefy_databarn.common.events import AnyEvent
from tap_beefy_databarn.common.pydantic_dataclass_stream import PydanticDataclassStream
from tap_beefy_databarn.squid_network_data.squid_config import get_squid_archive_url
from tap_beefy_databarn.squid_network_data.squid_models import SquidArchiveBlockResponse

EventType = t.Literal[
    "IERC20_Transfer",
    "BeefyZapRouter_FulfilledOrder",
]


class ContractEventWatch(BaseModel):
    chain: ChainType
    contract_address: str
    events: t.Iterable[EventType]
    creation_block_number: int
    creation_block_datetime: datetime.datetime


class SquidContext(BaseModel):
    chain: ChainType


class SquidImportState(BaseModel):
    chain: ChainType
    last_seen_height: int
    watched_contract: t.Iterable[ContractEventWatch]


class SquidEventStreamRecord(BaseModel):
    unique_key: str
    chain: ChainType
    last_seen_height: int
    # watched_contract: list[ContractEventWatch]
    event: AnyEvent | None  # None when we just want to update the last_seen_height


class SquidContractEventsStream(PydanticDataclassStream):
    """Fetches the contract creation date for a given contract address."""

    name = "squid_event_stream"
    record_dataclass = SquidEventStreamRecord
    primary_keys: t.ClassVar[list[str]] = ["unique_key"]
    is_sorted = False
    replication_method = "INCREMENTAL"
    replication_key = "last_seen_height"

    @property
    def partitions(self) -> list[dict] | None:
        return [{"chain": c} for c in all_chains]

    def get_records(self, context: dict[t.Any, t.Any] | None) -> t.Iterable[dict]:
        last_seen_heigth = self.get_starting_replication_key_value(context or {}) or 0
        squid_context = SquidContext(**t.cast(dict[t.Any, t.Any], context))

        self.logger.info("Fetching squid events for chain %s from block %s", squid_context.chain, last_seen_heigth)

        contracts = self._get_watch_list(squid_context)
        if not contracts:
            self.logger.info("No contracts to watch for chain %s", squid_context.chain)
            return
        state = SquidImportState(chain=squid_context.chain, last_seen_height=last_seen_heigth, watched_contract=contracts)

        for event in self._get_records_for_chain(state):
            yield self._pydantic_dataclass_to_dict(event)

    def _get_watch_list(self, context: SquidContext) -> t.Iterable[ContractEventWatch]:
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
                WHERE chain = %(chain)s
            """,
                {"chain": context.chain},
            )
            return cur.fetchall()

    def _get_url_as_text(self, url: str) -> str:
        """Fetch the url and return the text."""
        # TODO: add retry logic and extract in a common client class
        self.logger.debug("Fetching %s", url)
        res = requests.get(url, timeout=10)
        if res.status_code != 200:
            msg = f"Got status code {res.status_code} when fetching {url}: {res.text}"
            raise Exception(msg)
        self.logger.debug("Got response %s", res.text)
        return res.text

    def _post_url_as_json(self, url: str, query: dict) -> dict:
        """Fetch the url and return the json."""
        # TODO: add retry logic and extract in a common client class
        self.logger.debug("Fetching %s with query %s", url, query)
        res = requests.post(url, json=query, timeout=10)
        if res.status_code != 200:
            msg = f"Got status code {res.status_code} when fetching {url}: {res.text}"
            raise Exception(msg)
        self.logger.debug("Got json response")
        return res.json()

    def _get_records_for_chain(self, state: SquidImportState) -> t.Iterable[SquidEventStreamRecord]:
        """Get the records for a given chain."""

        # first, identify the first block to fetch
        min_contract_creation_block = min([c.creation_block_number for c in state.watched_contract])
        start_block = max(min_contract_creation_block, state.last_seen_height)

        # get the archive node url
        archive_url = get_squid_archive_url(state.chain)
        if archive_url is None:
            self.logger.info("No archive node url for chain %s", state.chain)
            return

        archived_height = int(self._get_url_as_text(f"{archive_url}/height"))
        next_block = start_block
        last_block = archived_height
        self.logger.debug("Archived height for chain %s is %s", state.chain, archived_height)

        # TODO: derive this from the watched contract abi
        topics_map = {
            "IERC20_Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "BeefyZapRouter_FulfilledOrder": "0x1ba5b6ed656994657175705961138c96bd8ec133c35817fa85903f450129e0b1",
        }
        event_types = list({e for c in state.watched_contract for e in c.events})
        event_topics = [topics_map[e] for e in event_types]

        # https://docs.subsquid.io/subsquid-network/reference/evm-api/
        query = {
            "logs": [{"address": [c.contract_address for c in state.watched_contract], "topic0": event_topics, "transaction": True}],
            "fields": SquidArchiveBlockResponse.get_archive_query_fields(),
            "fromBlock": next_block,
            "toBlock": last_block,
        }

        while next_block <= last_block:
            worker_url = self._get_url_as_text(f"{archive_url}/{next_block}/worker")
            self.logger.debug("Fetching events for chain %s from block %s to %s using worker %s", state.chain, next_block, last_block, worker_url)

            query["fromBlock"] = next_block
            query["toBlock"] = last_block

            blocks = self._post_url_as_json(worker_url, query)

            last_processed_block = blocks[-1]["header"]["number"]
            next_block = last_processed_block + 1
            for block in blocks:
                self.logger.info("Processing block %s", block)
                squid_block_response = SquidArchiveBlockResponse.model_validate(block)
                self.logger.debug("Got block %s", squid_block_response)
                for tx in block["transactions"]:
                    self.logger.info("Processing tx %s", tx)
                    block_number = 12
                    log_index = 0
                    unique_key = f"{state.chain}-{block_number}-{log_index}"
                    yield SquidEventStreamRecord(unique_key=unique_key, chain=state.chain, last_seen_height=0, event=None)
                    return
