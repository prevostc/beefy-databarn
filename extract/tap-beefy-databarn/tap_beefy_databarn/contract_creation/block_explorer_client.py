

from __future__ import annotations

import typing as t
from abc import abstractmethod
from datetime import UTC, datetime

from dateutil import parser
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Timeout
from urllib3.util import Retry

from tap_beefy_databarn.common.explorer_config import EXPLORER_CONFIG, ExplorerConfig, ExplorerType
from tap_beefy_databarn.contract_creation.contract_creation_models import ContractCreationInfo, ContractWatch
from tap_beefy_databarn.http.rate_limit import InMemoryRateLimitMixin
from tap_beefy_databarn.http.response_code import ResponseCodeMixin
from tap_beefy_databarn.http.retry import RetryMixin
from tap_beefy_databarn.http.timeout import TimeoutMixin

if t.TYPE_CHECKING:
    from logging import Logger

    from tap_beefy_databarn.common.chains import Chain


class BlockExplorerClient:

    session: Session
    explorer_config: ExplorerConfig
    logger: Logger

    def __init__(self, logger: Logger, explorer_config: ExplorerConfig) -> None:

        class ExplorerHTTPAdapter(ResponseCodeMixin, RetryMixin, InMemoryRateLimitMixin, TimeoutMixin, HTTPAdapter): # type: ignore  # noqa: PGH003
            """
            A session with a rate limiter, a timeout, and a retry policy in order
            this is somewhat equivalent to retry(rate_limiter(timeout(httpAdapter)))
            we want to make sure retry is applied including the rate limiter
            and timeout is applied last.
            """

        adapter = ExplorerHTTPAdapter(
            timeout=Timeout(connect=5, read=10),
            per_second=explorer_config.max_rps,
            max_retries=Retry(
                total=3,
                backoff_factor=5.0,
                status_forcelist=[502, 503, 504],
                allowed_methods={"POST", "GET"},
            ),
            expected_response_code=200,
        )

        session = Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        self.session = session
        self.explorer_config = explorer_config
        self.logger = logger

    @classmethod
    def from_chain(cls, parent_logger: Logger, chain: Chain) -> BlockExplorerClient:  # noqa: ANN102
        """Get a BlockExplorerClient for a specific chain"""
        explorer_config = EXPLORER_CONFIG[chain]
        logger = parent_logger.getChild(f"explorer.{chain}")
        class_map: dict[ExplorerType, type[BlockExplorerClient]] = {
            ExplorerType.ETHERSCAN: EtherscanClient,
            ExplorerType.SNOWTRACE: SnowtraceBlockExplorerClient,
            ExplorerType.BLOCKSCOUT: BlockscoutClient,
            ExplorerType.BLOCKSCOUT_V5: BlockscoutV5Client,
            ExplorerType.BLOCKSCOUT_TRX_LIST_API: BlockscoutTransactionListApiClient,
            ExplorerType.ZKSYNC: ZksyncExplorerClient,
        }

        if explorer_config.explorer_type not in class_map:
            msg = f"Unsupported explorer type: {explorer_config.explorer_type}"
            raise Exception(msg)

        cls_to_instanciate = class_map[explorer_config.explorer_type]
        return cls_to_instanciate(explorer_config=explorer_config, logger=logger)

    def get_contract_creation_infos(self, contracts: t.Iterable[ContractWatch]) -> t.Iterable[ContractCreationInfo]:
        """Get the contract creation info from any explorer"""
        for contract in contracts:
            try:
                yield self._get_contract_creation_info(contract=contract)
            except Exception as exc:
                self.logger.exception("Error while fetching contract creation info for %s:%s: %s", contract.chain, contract.contract_address, exc_info=exc)

    @abstractmethod
    def _get_contract_creation_info(self, contract: ContractWatch) -> ContractCreationInfo:
        """Get the contract creation info from any explorer"""
        raise NotImplementedError


class CouldNotParseExplorerResponseError(Exception):
    def __init__(self, response: dict) -> None:
        super().__init__(f"Could not parse explorer response: {response}")
        self.response = response


class EtherscanClient(BlockExplorerClient):

    def _get_contract_creation_info(self, contract: ContractWatch) -> ContractCreationInfo:
        """Get the contract creation info from Etherscan-like api."""

        params: t.Any = {
            "module": "account",
            "action": "txlist",
            "address": contract.contract_address,
            "startblock": 1,
            "endblock": 999999999,
            "page": 1,
            "offset": 1,
            "sort": "asc",
            "limit": 1,
        }

        data = self.session.get(self.explorer_config.url, params=params).json()

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
            chain=contract.chain,
            contract_address=contract.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )


class SnowtraceBlockExplorerClient(BlockExplorerClient):

    def _get_contract_creation_info(self, contract: ContractWatch) -> ContractCreationInfo:
        # https://snowtrace.dev/api/blockchain/43114/address/0x595786A3848B1de66C6056C87BA91977935fBC46/contract?ecosystem=43114
        # https://api.routescan.io/v2/network/mainnet/evm/43114/address/0x595786A3848B1de66C6056C87BA91977935fBC46/transactions?ecosystem=avalanche&includedChainIds=43114&categories=evm_tx&sort=asc&limit=1
        chain_id = 43114
        params: t.Any = {
            "ecosystem": chain_id,
        }
        api_path = f"{self.explorer_config.url}/blockchain/{chain_id}/address/{contract.contract_address}/contract"
        data = self.session.get(api_path, params=params).json()

        if "createdAt" not in data:
            msg = "Error from Routescan api: no 'createdAt' field, data: %s", data
            raise Exception(msg)

        self.logger.debug("Got data from Routescan api for (%s:%s): %s", contract.chain, contract.contract_address, data["createdAt"])

        block_number = data["createdAt"]["blockNumber"]
        block_datetime_str = data["createdAt"]["timestamp"]
        block_datetime = parser.parse(block_datetime_str)
        block_datetime = block_datetime.astimezone(UTC)

        return ContractCreationInfo(
            chain=contract.chain,
            contract_address=contract.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )


class BlockscoutV5Client(BlockExplorerClient):
    def _get_contract_creation_info(self, contract: ContractWatch) -> ContractCreationInfo:
        # https://explorer.plexnode.wtf/api/v2/addresses/0x5B19bd330A84c049b62D5B0FC2bA120217a18C1C
        api_path = f"{self.explorer_config.url}/v2/addresses/{contract.contract_address}"
        data = self.session.get(api_path).json()
        creation_tx = data["creation_tx_hash"]

        # https://explorer.plexnode.wtf/api/v2/transactions/0xb76958d652c19beba8e96750a5fab1054a2170274a60e5ea54f64954058dcd94
        api_path = f"{self.explorer_config.url}/v2/transactions/{creation_tx}"
        data = self.session.get(api_path).json()

        block_number = data["block"]
        block_datetime_str = data["timestamp"]
        block_datetime = parser.parse(block_datetime_str)
        block_datetime = block_datetime.astimezone(UTC)

        return ContractCreationInfo(
            chain=contract.chain,
            contract_address=contract.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )

class BlockscoutTransactionListApiClient(BlockExplorerClient):
    def _get_contract_creation_info(self, contract: ContractWatch) -> ContractCreationInfo:
        data = {"items": [], "next_page_path": f"/address/{contract.contract_address}/transactions"}

        while data["next_page_path"]:
            api_path = f"{self.explorer_config.url}{data['next_page_path']}"
            if "type=JSON" not in api_path:
                api_path += "?" if "?" not in api_path else "&"
                api_path += "type=JSON"

            data = self.session.get(api_path).json()

        if len(data["items"]) == 0:
            msg = "Error from Blockscout JSON api: no items"
            raise Exception(msg)

        tx = data["items"][-1]
        self.logger.debug("Got tx from Blockscout JSON api for (%s:%s): %s", contract.chain, contract.contract_address, tx)
        block_number_str = tx.split('href="/block/')[1].split('"')[0]
        block_number = int(block_number_str, 10)

        block_datetime_str = tx.split('data-from-now="')[1].split('"')[0]
        block_datetime = parser.parse(block_datetime_str)

        return ContractCreationInfo(
            chain=contract.chain,
            contract_address=contract.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )


class BlockscoutClient(BlockscoutTransactionListApiClient):
    def _get_contract_creation_info(self, contract: ContractWatch) -> ContractCreationInfo:
        # https://andromeda-explorer.metis.io/address/0x01A3c8E513B758EBB011F7AFaf6C37616c9C24d9
        data = self.session.get(f"{self.explorer_config.url}/address/{contract.contract_address}").json()

        # if the contract is unverified we need to fetch the transaction hash from the internal transaction log
        if "transaction_hash_link" not in data:
            return super()._get_contract_creation_info(contract)

        tx_hash = data.split('data-test="transaction_hash_link"')[1]

        if 'href="/tx/' in tx_hash:
            tx_hash = tx_hash.split('href="/tx/')[1].split('"')[0]
        elif 'href="/mainnet/tx' in tx_hash:
            # celo specific
            tx_hash = tx_hash.split('href="/mainnet/tx/')[1].split('"')[0]
        else:
            raise CouldNotParseExplorerResponseError({"tx_hash": tx_hash})

        # https://andromeda-explorer.metis.io/tx/0x9bbc84d8b646db3416a40bfdbb5a0028f93eb7a867f7ea6bb81f80c383d0bf28
        data = self.session.get(f"{self.explorer_config.url}/tx/{tx_hash}").json()

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
            chain=contract.chain,
            contract_address=contract.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )

class ZksyncExplorerClient(BlockExplorerClient):
    def _get_contract_creation_info(self, contract: ContractWatch) -> ContractCreationInfo:
        # https://block-explorer-api.mainnet.zksync.io/address/0x923C15333516A8784BfA77b235bFA92Ac649B889
        api_path = f"{self.explorer_config.url}/address/{contract.contract_address}"
        data = self.session.get(api_path).json()
        create_tx = data["creatorTxHash"]

        # https://block-explorer-api.mainnet.zksync.io/transactions/0x07a9ac0a93474d66c7a849a28424e6d480587d407cd6291657b1b2ab1ee68788
        api_path = f"{self.explorer_config.url}/transactions/{create_tx}"
        data = self.session.get(api_path).json()

        block_number = data["blockNumber"]
        block_datetime = data["receivedAt"]
        block_datetime = parser.parse(block_datetime)

        return ContractCreationInfo(
            chain=contract.chain,
            contract_address=contract.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )
