import typing as t
from dataclasses import dataclass
from datetime import UTC, datetime

import aiohttp
import psycopg

from extract.common.chains import ChainType
from extract.common.config import POSTGRES_CONNECTION_STRING
from extract.common.explorer_config import EXPLORER_CONFIG
from extract.common.logger import logger
from extract.common.rate_limit import rate_limit_iterator


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


async def _get_contract_list() -> t.AsyncIterable[ContractWatch]:
    """Get the list of contracts to watch for metadata."""
    async with await psycopg.AsyncConnection.connect(POSTGRES_CONNECTION_STRING) as aconn, aconn.cursor() as acur:
        await acur.execute("""
            SELECT
                chain,
                contract_address
            FROM analytics.contract_metadata_watchlist
        """)
        async for record in acur:
            yield ContractWatch(
                chain=record[0],
                contract_address=record[1],
            )

async def _get_from_etherscan(url: str, watch: ContractWatch) -> ContractCreationInfo:
    """Get the contract creation info from Etherscan-like api."""

    logger.debug("Getting contract creation info from Etherscan-like api (%s, %s, %s)", watch.chain, watch.contract_address, url)

    params = {
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

    async with aiohttp.ClientSession() as session, session.get(url, params=params) as resp:
        data = await resp.json()

        logger.debug("Got response from Etherscan-like api (%s, %s, %s): %s", watch.chain, watch.contract_address, url, data)

        if data["status"] == "0":
            msg = f"Error from Etherscan-like api: {data['message']}"
            raise Exception(msg)

        block_number = data["result"][0]["blockNumber"]
        if isinstance(block_number, str):
            block_number = int(block_number, 10)

        block_datetime = data["result"][0]["timeStamp"]
        if isinstance(block_datetime, str):
            block_datetime = int(block_datetime, 10)

        block_datetime = datetime.fromtimestamp(block_datetime, tz=UTC)

        return ContractCreationInfo(
            chain=watch.chain,
            contract_address=watch.contract_address,
            block_number=block_number,
            block_datetime=block_datetime,
        )


async def _get_contract_creation_infos() -> t.AsyncIterable[ContractCreationInfo]:
    """ Get the contract creation infos from any explorer"""
    by_type: dict[ChainType, list[ContractWatch]] = {}
    async for contract in _get_contract_list():
        by_type.setdefault(contract.chain, []).append(contract)

    for chain, contracts in by_type.items():
        explorer_config = EXPLORER_CONFIG[chain]
        if explorer_config.explorer_type == "etherscan":
            async for contract in rate_limit_iterator(contracts, max_rps=explorer_config.max_rps):
                contract_creation_info = await _get_from_etherscan(explorer_config.url, contract)
                yield contract_creation_info


async def main() -> None:
    """Main entrypoint."""
    async for contract_creation_info in _get_contract_creation_infos():
        logger.info("Got contract creation info: %s", contract_creation_info)

        # persist to database
        async with await psycopg.AsyncConnection.connect(POSTGRES_CONNECTION_STRING) as aconn, aconn.cursor() as acur:
            await acur.execute("""
                INSERT INTO contract_metadata.contract_metadata_creation_info (
                    chain,
                    contract_address,
                    block_number,
                    block_datetime
                ) VALUES (
                    %(chain)s,
                    %(contract_address)s,
                    %(block_number)s,
                    %(block_datetime)s
                ) ON CONFLICT (chain, contract_address) DO UPDATE SET
                    block_number = EXCLUDED.block_number,
                    block_datetime = EXCLUDED.block_datetime
            """, {
                "chain": contract_creation_info.chain,
                "contract_address": contract_creation_info.contract_address,
                "block_number": contract_creation_info.block_number,
                "block_datetime": contract_creation_info.block_datetime,
            })

# run the script
if __name__ == "__main__":
    import asyncio

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
