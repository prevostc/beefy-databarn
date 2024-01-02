"""Stream type classes for tap-beefy-databarn."""

from __future__ import annotations

import json
import typing as t
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import requests
from psycopg.rows import class_row
from singer_sdk.streams import Stream
from tap_beefy_databarn.common.explorer_config import EXPLORER_CONFIG
from tap_beefy_databarn.common.rate_limit import rate_limit_iterator

if t.TYPE_CHECKING:

    from tap_beefy_databarn.common.chains import ChainType



@dataclass
class ContractWatch:
    chain: ChainType
    contract_address: str

@dataclass
class ContractCreationInfo:
    chain: ChainType
    contract_address: str
    block_number: int
    block_datetime: datetime

class ContractCreationDateStream(Stream):
    """Fetches the contract creation date for a given contract address."""

    name = "contract_creation_date"
    primary_keys: t.ClassVar[list[str]] = ["chain", "contract_address"]
    replication_key = None
    schema_filepath = Path(__file__).parent / "./contract_creation_date.json"

    @property
    def schema(self) -> dict:
        return json.loads(Path(self.schema_filepath).read_text())

    def get_records(self, context: dict[t.Any, t.Any]|None) -> t.Iterable[dict]:  # noqa: ARG002
        self.logger.info("Fetching contract creation date: %s", self.schema)
        for infos in self._get_contract_creation_info():
            yield asdict(infos)

    def _get_contract_creation_info(self) -> t.Iterable[ContractCreationInfo]:
        """ Get the contract creation infos from any explorer"""
        by_type: dict[ChainType, list[ContractWatch]] = {}
        for contract in self._get_contract_list():
            by_type.setdefault(contract.chain, []).append(contract)

        for chain, contracts in by_type.items():
            explorer_config = EXPLORER_CONFIG[chain]
            if explorer_config.explorer_type == "etherscan":
                for contract in rate_limit_iterator(contracts, max_rps=explorer_config.max_rps):
                    contract_creation_info = self._get_from_etherscan(explorer_config.url, contract)
                    yield contract_creation_info

    def _get_contract_list(self) -> t.Iterable[ContractWatch]:
        """Get the list of contracts to watch for metadata."""
        connection_string = self.config.get("postgres_connection_string", "")
        with psycopg.Connection.connect(connection_string) as conn, conn.cursor(row_factory=class_row(ContractWatch)) as cur:
            cur.execute("""
                SELECT
                    chain,
                    contract_address
                FROM analytics.contract_metadata_watchlist
            """)
            return cur.fetchall()


    def _get_from_etherscan(self, url: str, watch: ContractWatch) -> ContractCreationInfo:
        """Get the contract creation info from Etherscan-like api."""

        params: t.Any = {
            "module": "account",
            "action": "txlist",
            "address": watch.contract_address,
            "startblock": 1,
            "endblock": 999999999,
            "page": 1,
            "offset": 1,
            "sort": "asc",
            "limit": 1,
        }

        res = requests.get(url, params=params, timeout=5)

        if res.status_code != 200:
            msg = f"Error from Etherscan-like api: {res.status_code}"
            raise Exception(msg)

        data = res.json()

        self.logger.debug("Got data from Etherscan-like api for (%s:%s): %s", watch.chain, watch.contract_address, data)

        if data.get("status") != "1":
            msg = f"Error from Etherscan-like api: {data.get('message')}"
            raise Exception(msg)

        data = data.get("result")[0]

        block_number = data.get("blockNumber")
        if isinstance(block_number, str):
            block_number = int(block_number, 10)

        block_datetime = data["timeStamp"]
        if isinstance(block_datetime, str):
            block_datetime = int(block_datetime, 10)

        block_datetime = datetime.fromtimestamp(block_datetime, tz=UTC)

        return ContractCreationInfo(
            chain=watch.chain,
            contract_address=watch.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )
