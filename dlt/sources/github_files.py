from __future__ import annotations
import logging
from typing import Any, AsyncIterator, Dict
import dlt
from lib.fetch import fetch_url_json_dict, fetch_url_json_list

logger = logging.getLogger(__name__)

@dlt.source(
    name="github_files", 
    max_table_nesting=0, 
    parallelized=True
)
async def github_files() -> Any:
    """Expose GitHub files resources for use by dlt pipelines."""

    @dlt.resource(
        name="beefy_platforms",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    )
    async def beefy_platforms() -> AsyncIterator[Dict[str, Any]]:
        async for item in fetch_url_json_list("https://raw.githubusercontent.com/beefyfinance/beefy-v2/refs/heads/main/src/config/platforms.json"):
            yield item

    return (beefy_platforms())
