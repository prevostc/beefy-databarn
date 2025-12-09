import logging
from typing import Any
import dlt
from dlt.sources.sql_database import sql_table
from lib.config import BATCH_SIZE, get_beefy_db_url
from .incremental.harvests import beefy_incremental_harvests
from .incremental.prices import beefy_incremental_prices

logger = logging.getLogger(__name__)


@dlt.source(name="beefy_db_incremental", parallelized=True)
async def beefy_db_incremental() -> Any:
    return (
        await beefy_incremental_harvests(),
        await beefy_incremental_prices(),
    )


@dlt.source(name="beefy_db_configs", parallelized=True)
async def beefy_db_configs() -> Any:
    """Expose Beefy DB resources for use by dlt pipelines."""

    db_url = get_beefy_db_url()

    tables = {
        "address_metadata": [
            {"name": "chain_id", "primary_key": True },
            {"name": "address", "primary_key": True },
        ],
        "bifi_buyback": [
            {"name": "id", "primary_key": True },
            {"name": "bifi_amount", "data_type": "decimal" },
            {"name": "bifi_price", "data_type": "decimal" },
            {"name": "buyback_total", "data_type": "decimal" },
        ],
        "chains": [
            {"name": "chain_id", "primary_key": True },
        ],
        "price_oracles": [{"name": "id", "primary_key": True }],
        "vault_ids": [{"name": "id", "primary_key": True }],
        "vault_strategies": [
            {"name": "id", "primary_key": True },
        ],
        "feebatch_harvests": [
            {"name": "chain_id", "primary_key": True },
            {"name": "block_number", "primary_key": True },
        ]
    }

    resources = []

    for table_name, columns in tables.items():
        primary_key_columns = [column["name"] for column in columns if "primary_key" in column and column["primary_key"]]
        resource = sql_table(
            credentials=db_url,
            table=table_name,
            backend="sqlalchemy",
            chunk_size=BATCH_SIZE,
            backend_kwargs={"tz": "UTC"},
            reflection_level="full_with_precision",
            primary_key=primary_key_columns,
            write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        )
        resource.apply_hints(columns=columns)
        resources.append(resource)

    return resources
