"""Stream type classes for tap-beefy-databarn."""

from __future__ import annotations

import json
import typing as t
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from dateutil import parser
from pathlib import Path

import psycopg
import requests
from psycopg.rows import class_row
from singer_sdk.streams import Stream
from tap_beefy_databarn.common.explorer_config import EXPLORER_CONFIG
from tap_beefy_databarn.common.rate_limit import rate_limit_iterator, sleep_rps

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

        fn_map = {
            "etherscan": self._get_from_etherscan,
            "routescan": self._get_from_routescan,
            "blockscout-trx-list-api": self._get_from_blockscout_transaction_list_api,
            "blockscout-v5": self._get_from_blockscout_v5,
            "blockscout": self._get_from_blockscout,
            "zksync": self._get_from_zksync,
        }

        for chain, contracts in by_type.items():
            explorer_config = EXPLORER_CONFIG[chain]
            
            if explorer_config.explorer_type not in fn_map:
                self.logger.warning("No explorer function for %s", explorer_config.explorer_type)
                continue

            fn = fn_map[explorer_config.explorer_type]
            for contract in rate_limit_iterator(contracts, explorer_config.max_rps):
                try:
                    yield fn(explorer_config, contract)
                except Exception as exc:
                    self.logger.error("Error while fetching contract creation info for %s:%s: %s", contract.chain, contract.contract_address, exc, exc_info=True)


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
            msg = f"Error from Routescan api: no items"
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
        data = {
            "items": [],
            "next_page_path": f"/address/{watch.contract_address}/transactions"
        }

        while data["next_page_path"]:
            api_path = f"{explorer_config.url}{data['next_page_path']}"
            if "type=JSON" not in api_path:
                api_path += "?" if "?" not in api_path else "&"
                api_path += "type=JSON"

            data = self._fetch_json_or_throw(api_path)
            sleep_rps(explorer_config.max_rps)
                
        if len(data["items"]) == 0:
            msg = f"Error from Blockscout JSON api: no items"
            raise Exception(msg)

        tx = data["items"][-1]
        self.logger.debug("Got tx from Blockscout JSON api for (%s:%s): %s", watch.chain, watch.contract_address, tx)
        block_number = tx.split('href="/block/')[1].split('"')[0]
        block_number = int(block_number, 10)

        block_datetime = tx.split('data-from-now="')[1].split('"')[0]
        block_datetime = parser.parse(block_datetime)

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
        block_datetime = data["timestamp"]
        block_datetime = parser.parse(block_datetime)

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

        tx_hash = data.split('data-test="transaction_hash_link"')[1].split('href="/tx/')[1].split('"')[0]

        # https://andromeda-explorer.metis.io/tx/0x9bbc84d8b646db3416a40bfdbb5a0028f93eb7a867f7ea6bb81f80c383d0bf28
        sleep_rps(explorer_config.max_rps)
        data = self._fetch_html_or_throw(f"{explorer_config.url}/tx/{tx_hash}")
        block_number = data.split('class="transaction__link" href="/block/')[1].split('"')[0]
        block_number = int(block_number, 10)

        block_datetime = data.split('data-from-now="')[1].split('"')[0]
        block_datetime = parser.parse(block_datetime)

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