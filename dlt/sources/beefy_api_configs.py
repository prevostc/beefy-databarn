from __future__ import annotations
import logging
from typing import Any, AsyncIterator, Dict
import dlt
from lib.fetch import fetch_url_json_dict, fetch_url_json_list

logger = logging.getLogger(__name__)

@dlt.source(
    name="beefy_api_configs", 
    max_table_nesting=0, 
    parallelized=True
)
async def beefy_api_configs() -> Any:
    """Expose Beefy Config API resources for use by dlt pipelines."""

    @dlt.resource(
        name="vaults",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    )
    async def beefy_vaults() -> AsyncIterator[Dict[str, Any]]:
        async for item in fetch_url_json_list("https://api.beefy.finance/vaults"):
            # prevent crashes where python tries to convert the total supply to an Int and it's too large
            if "totalSupply" in item:
                item["totalSupply"] = str(item["totalSupply"])
            yield item


    @dlt.resource(
        name="gov_vaults",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    )
    async def beefy_gov_vaults() -> AsyncIterator[Dict[str, Any]]:
        async for item in fetch_url_json_list("https://api.beefy.finance/gov-vaults"):
            # prevent crashes where python tries to convert the total supply to an Int and it's too large
            if "totalSupply" in item:
                item["totalSupply"] = str(item["totalSupply"])
            yield item


    @dlt.resource(
        name="boosts",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    )
    async def beefy_boosts() -> AsyncIterator[Dict[str, Any]]:
        async for item in fetch_url_json_list("https://api.beefy.finance/boosts"):
            yield item


    @dlt.resource(
        name="clm_vaults",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        columns={
            "feeTier": {"data_type": "text"},
        },
    )
    async def beefy_clm_vaults() -> AsyncIterator[Dict[str, Any]]:
        async for item in fetch_url_json_list("https://api.beefy.finance/clm-vaults"):
            # prevent crashes where python tries to convert the total supply to an Int and it's too large
            if "totalSupply" in item:
                item["totalSupply"] = str(item["totalSupply"])

            if "vault" in item:
                if "totalSupply" in item["vault"]:
                    item["vault"]["totalSupply"] = str(item["vault"]["totalSupply"])
            if "pool" in item:
                if "totalSupply" in item["pool"]:
                    item["pool"]["totalSupply"] = str(item["pool"]["totalSupply"])
            yield item


    @dlt.resource(
        name="cow_vaults",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    )
    async def beefy_cow_vaults() -> AsyncIterator[Dict[str, Any]]:
        async for item in fetch_url_json_list("https://api.beefy.finance/cow-vaults"):
            # prevent crashes where python tries to convert the total supply to an Int and it's too large
            if "totalSupply" in item:
                item["totalSupply"] = str(item["totalSupply"])
            yield item


    @dlt.resource(
        name="tokens",
        primary_key=["chainId", "id"],
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    )
    async def beefy_tokens() -> AsyncIterator[Dict[str, Any]]:
        payload, _ = await fetch_url_json_dict("https://api.beefy.finance/tokens")
        
        # Flatten the nested structure: iterate through chains and tokens
        for chain_id, tokens in payload.items():
            for token_id, token_data in tokens.items():
                # Ensure chainId is set in the token data
                token_data["chainId"] = chain_id
                yield token_data


    return (beefy_vaults(), beefy_gov_vaults(), beefy_clm_vaults(), beefy_cow_vaults(), beefy_boosts(), beefy_tokens())
