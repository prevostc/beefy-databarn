"""Stream type classes for tap-beefy-databarn."""

from __future__ import annotations

import typing as t
from typing import Any

import psycopg
from psycopg.rows import class_row
from tap_beefy_databarn.common.chains import Chain
from tap_beefy_databarn.contract_creation.block_explorer_client import BlockExplorerClient
from tap_beefy_databarn.contract_creation.contract_creation_models import ContractCreationInfo, ContractWatch
from tap_beefy_databarn.singer.pydantic_multithread_stream import PydanticMultithreadStream

ThreadParams = tuple[Chain, list[ContractWatch]]

class ContractCreationDateStream(PydanticMultithreadStream[ThreadParams, ContractCreationInfo]):
    """Fetches the contract creation date for a given contract address."""

    record_dataclass = ContractCreationInfo
    name = "contract_creation_date"
    primary_keys: t.ClassVar[list[str]] = ["chain", "contract_address"]
    replication_key = None

    def _get_contract_list(self) -> t.Iterable[ContractWatch]:
        """Get the list of contracts to watch for metadata."""
        connection_string = self.config.get("postgres_connection_string", "")
        with psycopg.Connection.connect(connection_string) as conn, conn.cursor(row_factory=class_row(ContractWatch)) as cur:
            cur.execute(
                """
                SELECT
                    chain,
                    contract_address
                FROM analytics.contract_metadata_watchlist
            """,
            )
            return cur.fetchall()

    def get_thread_params(self, _: dict[Any, Any] | None) -> t.Iterable[ThreadParams]:
        """Get the contract creation infos from any explorer"""
        chain_filter: Chain | None = self.config.get("chain")
        contract_address_filter: str | None = self.config.get("contract_address")
        self.logger.info("Fetching contract creation infos with filters: (chain=%s, contract_address=%s)", chain_filter, contract_address_filter)

        by_type: dict[Chain, list[ContractWatch]] = {}
        for contract in self._get_contract_list():
            if chain_filter and contract.chain != chain_filter:
                self.logger.debug("Skipping %s because of chain filter: %s", contract, chain_filter)
                continue
            if contract_address_filter and contract.contract_address.lower() != contract_address_filter.lower():
                self.logger.debug("Skipping %s because of contract address filter: %s", contract, contract_address_filter)
                continue
            by_type.setdefault(contract.chain, []).append(contract)

        yield from by_type.items()

    def thread_record_producer(self, _: dict[Any, Any] | None, params: ThreadParams) -> t.Iterable[ContractCreationInfo]:
        """Get the contract creation infos from any explorer"""

        (chain, contracts) = params
        client = BlockExplorerClient.from_chain(self.logger, chain)
        yield from client.get_contract_creation_infos(contracts)
