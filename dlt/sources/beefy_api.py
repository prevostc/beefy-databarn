from __future__ import annotations
import logging
from typing import Any
import dlt
from .resources.beefy_api.configs import get_beefy_api_configs_resources
from .resources.beefy_api.snapshots import get_beefy_api_snapshots_resources

logger = logging.getLogger(__name__)

@dlt.source(
    name="beefy_api", 
    max_table_nesting=0, 
    parallelized=True
)
async def beefy_api_source() -> Any:
    """Expose Beefy Config API resources for use by dlt pipelines."""

    configs_resources = await get_beefy_api_configs_resources()
    snapshots_resources = await get_beefy_api_snapshots_resources()

    return configs_resources + snapshots_resources

