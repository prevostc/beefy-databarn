"""Stream type classes for tap-beefy-databarn."""

from __future__ import annotations

import typing as t
from datetime import UTC, datetime
from typing import Any

import psycopg
import requests
from dateutil import parser
from psycopg.rows import class_row
from pydantic import BaseModel
from tap_beefy_databarn.common.chains import Chain
from tap_beefy_databarn.common.explorer_config import EXPLORER_CONFIG, ExplorerConfig
from tap_beefy_databarn.common.rate_limit import rate_limit_iterator, sleep_rps
from tap_beefy_databarn.singer.pydantic_multithread_stream import PydanticMultithreadStream


class ContractWatch(BaseModel):
    chain: Chain
    contract_address: str


class ContractCreationInfo(BaseModel):
    chain: Chain
    contract_address: str
    block_number: int
    block_datetime: datetime

ThreadParams = tuple[Chain, list[ContractWatch]]

class ContractCreationDateStream(PydanticMultithreadStream[ThreadParams, ContractCreationInfo]):
    """Fetches the contract creation date for a given contract address."""

    record_dataclass = ContractCreationInfo
    name = "contract_creation_date"
    primary_keys: t.ClassVar[list[str]] = ["chain", "contract_address"]
    replication_key = None

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

    def thread_record_producer(self, _: dict[Any, Any] | None, params: ThreadParams) -> t.Iterable[ContractCreationInfo]:
        """Get the contract creation infos from any explorer"""
        fn_map = {
            "etherscan": self._get_from_etherscan,
            "routescan": self._get_from_routescan,
            "blockscout-trx-list-api": self._get_from_blockscout_transaction_list_api,
            "blockscout-v5": self._get_from_blockscout_v5,
            "blockscout": self._get_from_blockscout,
            "zksync": self._get_from_zksync,
        }

        (chain, contracts) = params
        explorer_config = EXPLORER_CONFIG[chain]

        if explorer_config.explorer_type not in fn_map:
            self.logger.warning("No explorer function for %s", explorer_config.explorer_type)
            return

        fn = fn_map[explorer_config.explorer_type]
        for contract in rate_limit_iterator(contracts, explorer_config.max_rps):
            try:
                yield fn(explorer_config, contract)
            except Exception as exc:
                self.logger.exception("Error while fetching contract creation info for %s:%s: %s", contract.chain, contract.contract_address, exc_info=exc)


    def _get_from_etherscan(self, explorer_config: ExplorerConfig, watch: ContractWatch) -> ContractCreationInfo:
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

        data = self._fetch_json_or_throw(explorer_config.url, params)

        if data.get("status") != "1":
            msg = f"Error from Etherscan-like api: {data.get('message')}"
            raise Exception(msg)

        data = data["result"][0]

        block_number_str = data.get("blockNumber")
        if isinstance(block_number_str, str):
            block_number = int(block_number_str, 10)
        elif isinstance(block_number_str, int):
            block_number = block_number_str
        else:
            raise CouldNotParseExplorerResponseError({"blockNumber": block_number_str})

        block_datetime_str = data["timeStamp"]
        if isinstance(block_datetime_str, str):
            block_datetime_int = int(block_datetime_str, 10)
        elif isinstance(block_datetime_str, int):
            block_datetime_int = block_datetime_str
        else:
            raise CouldNotParseExplorerResponseError({"timeStamp": block_datetime_str})
        block_datetime = datetime.fromtimestamp(block_datetime_int, tz=UTC)

        return ContractCreationInfo(
            chain=watch.chain,
            contract_address=watch.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )

    def _get_from_routescan(self, explorer_config: ExplorerConfig, watch: ContractWatch) -> ContractCreationInfo:
        # https://api.routescan.io/v2/network/mainnet/evm/43114/address/0x595786A3848B1de66C6056C87BA91977935fBC46/transactions?ecosystem=avalanche&includedChainIds=43114&categories=evm_tx&sort=asc&limit=1
        params: t.Any = {
            "ecosystem": "avalanche",
            "includedChainIds": 43114,
            "categories": "evm_tx",
            "sort": "asc",
            "limit": 1,
        }
        api_path = f"{explorer_config.url}/v2/network/mainnet/evm/43114/address/{watch.contract_address}/transactions"
        data = self._fetch_json_or_throw(api_path, params)

        self.logger.debug("Got data from Routescan api for (%s:%s): %s", watch.chain, watch.contract_address, data)
        if "items" not in data or len(data["items"]) == 0:
            msg = "Error from Routescan api: no items"
            raise Exception(msg)

        block_number = data["items"][0]["blockNumber"]
        if isinstance(block_number, str):
            block_number = int(block_number, 10)

        block_datetime = data["items"][0]["timestamp"]
        if isinstance(block_datetime, str):
            block_datetime = parser.parse(block_datetime)

        return ContractCreationInfo(
            chain=watch.chain,
            contract_address=watch.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )

    def _get_from_blockscout_transaction_list_api(self, explorer_config: ExplorerConfig, watch: ContractWatch) -> ContractCreationInfo:
        data = {"items": [], "next_page_path": f"/address/{watch.contract_address}/transactions"}

        while data["next_page_path"]:
            api_path = f"{explorer_config.url}{data['next_page_path']}"
            if "type=JSON" not in api_path:
                api_path += "?" if "?" not in api_path else "&"
                api_path += "type=JSON"

            data = self._fetch_json_or_throw(api_path)
            sleep_rps(explorer_config.max_rps)

        if len(data["items"]) == 0:
            msg = "Error from Blockscout JSON api: no items"
            raise Exception(msg)

        tx = data["items"][-1]
        self.logger.debug("Got tx from Blockscout JSON api for (%s:%s): %s", watch.chain, watch.contract_address, tx)
        block_number_str = tx.split('href="/block/')[1].split('"')[0]
        block_number = int(block_number_str, 10)

        block_datetime_str = tx.split('data-from-now="')[1].split('"')[0]
        block_datetime = parser.parse(block_datetime_str)

        return ContractCreationInfo(
            chain=watch.chain,
            contract_address=watch.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )

    def _get_from_blockscout_v5(self, explorer_config: ExplorerConfig, watch: ContractWatch) -> ContractCreationInfo:
        # https://explorer.plexnode.wtf/api/v2/addresses/0x5B19bd330A84c049b62D5B0FC2bA120217a18C1C
        api_path = f"{explorer_config.url}/v2/addresses/{watch.contract_address}"
        data = self._fetch_json_or_throw(api_path)
        creation_tx = data["creation_tx_hash"]

        # https://explorer.plexnode.wtf/api/v2/transactions/0xb76958d652c19beba8e96750a5fab1054a2170274a60e5ea54f64954058dcd94
        sleep_rps(explorer_config.max_rps)
        api_path = f"{explorer_config.url}/v2/transactions/{creation_tx}"
        data = self._fetch_json_or_throw(api_path)

        block_number = data["block"]
        block_datetime_str = data["timestamp"]
        block_datetime = parser.parse(block_datetime_str)

        return ContractCreationInfo(
            chain=watch.chain,
            contract_address=watch.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )

    def _get_from_blockscout(self, explorer_config: ExplorerConfig, watch: ContractWatch) -> ContractCreationInfo:
        # https://andromeda-explorer.metis.io/address/0x01A3c8E513B758EBB011F7AFaf6C37616c9C24d9
        data = self._fetch_html_or_throw(f"{explorer_config.url}/address/{watch.contract_address}")

        # if the contract is unverified we need to fetch the transaction hash from the internal transaction log
        if "transaction_hash_link" not in data:
            return self._get_from_blockscout_transaction_list_api(explorer_config, watch)

        tx_hash = data.split('data-test="transaction_hash_link"')[1]

        if 'href="/tx/' in tx_hash:
            tx_hash = tx_hash.split('href="/tx/')[1].split('"')[0]
        elif 'href="/mainnet/tx' in tx_hash:
            # celo specific
            tx_hash = tx_hash.split('href="/mainnet/tx/')[1].split('"')[0]
        else:
            raise CouldNotParseExplorerResponseError({"tx_hash": tx_hash})

        # https://andromeda-explorer.metis.io/tx/0x9bbc84d8b646db3416a40bfdbb5a0028f93eb7a867f7ea6bb81f80c383d0bf28
        sleep_rps(explorer_config.max_rps)
        data = self._fetch_html_or_throw(f"{explorer_config.url}/tx/{tx_hash}")

        block_number_str = data.split('class="transaction__link"')[1]
        if 'href="/block/' in block_number_str:
            block_number_str = block_number_str.split('href="/block/')[1].split('"')[0]
        elif 'href="/mainnet/block/' in block_number_str:
            # celo specific
            block_number_str = block_number_str.split('href="/mainnet/block/')[1].split('"')[0]
        else:
            raise CouldNotParseExplorerResponseError({"html": data})
        block_number = int(block_number_str, 10)

        block_datetime_str = data.split('data-from-now="')[1].split('"')[0]
        block_datetime = parser.parse(block_datetime_str)

        return ContractCreationInfo(
            chain=watch.chain,
            contract_address=watch.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )

    def _get_from_zksync(self, explorer_config: ExplorerConfig, watch: ContractWatch) -> ContractCreationInfo:
        # https://block-explorer-api.mainnet.zksync.io/address/0x923C15333516A8784BfA77b235bFA92Ac649B889
        api_path = f"{explorer_config.url}/address/{watch.contract_address}"
        data = self._fetch_json_or_throw(api_path)
        create_tx = data["creatorTxHash"]

        # https://block-explorer-api.mainnet.zksync.io/transactions/0x07a9ac0a93474d66c7a849a28424e6d480587d407cd6291657b1b2ab1ee68788
        sleep_rps(explorer_config.max_rps)
        api_path = f"{explorer_config.url}/transactions/{create_tx}"
        data = self._fetch_json_or_throw(api_path)

        block_number = data["blockNumber"]
        block_datetime = data["receivedAt"]
        block_datetime = parser.parse(block_datetime)

        return ContractCreationInfo(
            chain=watch.chain,
            contract_address=watch.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )

    def _fetch_json_or_throw(self, url: str, params: dict = {}) -> dict:
        self.logger.info("Fetching %s with params %s", url, params)
        res = requests.get(url, params=params, timeout=5)

        if res.status_code != 200:
            self.logger.error("Error from api (%s): %s", url, res.text)
            msg = f"Error from api: {res.status_code}"
            raise Exception(msg)

        data = res.json()
        self.logger.debug("Got data from api: %s", url)
        return data

    def _fetch_html_or_throw(self, url: str, params: dict = {}) -> str:
        self.logger.info("Fetching %s with params %s", url, params)
        res = requests.get(url, params=params, timeout=5)

        if res.status_code != 200:
            self.logger.error("Error from api (%s): %s", url, res.text)
            msg = f"Error from api: {res.status_code}"
            raise Exception(msg)

        self.logger.debug("Got data from api: %s", url)

        return res.text


class CouldNotParseExplorerResponseError(Exception):
    def __init__(self, response: dict) -> None:
        super().__init__(f"Could not parse explorer response: {response}")
        self.response = response
