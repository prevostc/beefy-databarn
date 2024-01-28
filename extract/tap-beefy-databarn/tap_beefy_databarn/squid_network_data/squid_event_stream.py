"""Stream type classes for tap-beefy-databarn."""

from __future__ import annotations

import typing as t
from datetime import datetime  # noqa: TCH003

import psycopg
import requests
from eth_typing.evm import BlockNumber, ChecksumAddress, HexAddress, HexStr
from psycopg.rows import class_row
from pydantic import BaseModel
from tap_beefy_databarn.common.chains import ChainType, all_chains
from tap_beefy_databarn.contract_event.event_data_parser import BeefyEventParser
from tap_beefy_databarn.contract_event.event_models import AnyEvent, EventType, event_event_type_to_topic0
from tap_beefy_databarn.singer.pydantic_dataclass_stream import PydanticDataclassStream
from tap_beefy_databarn.squid_network_data.squid_config import get_squid_archive_url
from tap_beefy_databarn.squid_network_data.squid_models import SquidArchiveBlockResponse
from web3.types import HexBytes, LogReceipt


class ContractEventWatch(BaseModel):
    chain: ChainType
    contract_address: str
    events: t.Iterable[EventType]
    creation_block_number: int
    creation_block_datetime: datetime


class SquidContext(BaseModel):
    chain: ChainType


class SquidImportState(BaseModel):
    chain: ChainType
    last_seen_height: int
    watched_contracts: list[ContractEventWatch]


class SquidEventStreamRecord(BaseModel):
    chain: ChainType
    last_seen_height: int
    # TODO: add the list of watched contracts to the state
    event: AnyEvent | None  # None when we just want to update the last_seen_height of the state

    @property
    def contract_address(self) -> str:
        if self.event is None:
            return ""
        return self.event.contract_address

    @property
    def block_number(self) -> int:
        if self.event is None:
            return 0
        return self.event.block_number

    @property
    def log_index(self) -> int:
        if self.event is None:
            return 0
        return self.event.log_index


class SquidContractEventsStream(PydanticDataclassStream[SquidEventStreamRecord]):
    """Fetches the contract creation date for a given contract address."""

    name = "squid_event_stream"
    record_dataclass = SquidEventStreamRecord
    primary_keys: t.ClassVar[list[str]] = ["chain", "contract_address", "block_number", "log_index"]
    is_sorted = False
    replication_method = "INCREMENTAL"
    replication_key = "last_seen_height"
    event_parser = BeefyEventParser()

    @property
    def partitions(self) -> list[dict] | None:
        return [{"chain": c} for c in all_chains]

    def get_models(self, context: dict[t.Any, t.Any] | None) -> t.Iterable[SquidEventStreamRecord]:
        last_seen_heigth = self.get_starting_replication_key_value(context or {}) or 0
        squid_context = SquidContext(**t.cast(dict[t.Any, t.Any], context))

        contracts = self._get_watch_list(squid_context)
        if not contracts:
            self.logger.info("No contracts to watch for chain %s", squid_context.chain)
            return
        state = SquidImportState(chain=squid_context.chain, last_seen_height=last_seen_heigth, watched_contracts=list(contracts))

        yield from self._get_records_for_chain(state)

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

    def _get_records_for_chain(self, state: SquidImportState) -> t.Iterable[SquidEventStreamRecord]:
        """Get the records for a given chain."""
        # first, identify the first block to fetch
        min_contract_creation_block = min([c.creation_block_number for c in state.watched_contracts])
        start_block = max(min_contract_creation_block, state.last_seen_height)
        assert start_block >= 0, f"start_block should be >= 0, got {start_block}"
        self.logger.info("Fetching events for chain %s from block %s", state.chain, start_block)

        # get the archive node url
        archive_url = get_squid_archive_url(state.chain)
        if archive_url is None:
            self.logger.info("No archive node url for chain %s", state.chain)
            return
        self.logger.debug("Archive url for chain %s is %s", state.chain, archive_url)

        archived_height = int(self._get_url_as_text(f"{archive_url}/height"))
        next_block = start_block
        last_block = archived_height
        self.logger.debug("Archived height for chain %s is %s", state.chain, archived_height)

        event_types = list({e for c in state.watched_contracts for e in c.events})
        assert event_types, "event_types should not be empty"

        event_topics = [event_event_type_to_topic0[e] for e in event_types]
        contract_addresses = [c.contract_address for c in state.watched_contracts]

        assert next_block <= last_block, f"next_block should be <= last_block, got {next_block} and {last_block}"
        assert len(event_topics) > 0, "event_topics should not be empty"
        assert len(contract_addresses) > 0, "contract_addresses should not be empty"

        # https://docs.subsquid.io/subsquid-network/reference/evm-api/
        query = {
            "logs": [{"address": contract_addresses, "topic0": event_topics, "transaction": True}],
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

            self.logger.info("Got %s blocks with %s logs", len(blocks), sum([len(b["logs"]) for b in blocks]))

            last_processed_block = blocks[-1]["header"]["number"]
            next_block = last_processed_block + 1
            for block in blocks:
                self.logger.debug("Processing block %s", block)
                squid_block_response = SquidArchiveBlockResponse.model_validate(block)
                for log in squid_block_response.logs:
                    self.logger.debug("Processing log %s", log)
                    log_receipt = LogReceipt(
                        topics=[HexBytes(topic) for topic in log.topics],
                        data=HexBytes(log.data),
                        logIndex=log.log_index,
                        blockHash=HexBytes(squid_block_response.block.block_hash),
                        blockNumber=BlockNumber(squid_block_response.block.number),
                        address=ChecksumAddress(HexAddress(HexStr(log.address))),
                        transactionIndex=log.transaction_index,
                        transactionHash=HexBytes(log.transaction_hash),
                        removed=False,
                    )
                    parsed_event = self.event_parser.parse_any_event(state.chain, log_receipt, squid_block_response.block.timestamp)
                    # set the last seen height to the block before the current one
                    # so we can retry the whole block on the next iteration
                    squid_block_response.block.number - 1
                    yield SquidEventStreamRecord(chain=state.chain, last_seen_height=0, event=parsed_event)

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
