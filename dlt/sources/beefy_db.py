import logging
from typing import Any
import datetime
import dlt
import psycopg2
import sqlalchemy as sa
from dlt.sources.sql_database import sql_table, sql_database
from lib.config import BATCH_SIZE

logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

DATASET_NAME = "beefy_db"


@dlt.source(name="beefy_db_configs")
async def beefy_db_configs() -> Any:
    """Expose Beefy DB resources for use by dlt pipelines."""

    tables = [
        "address_metadata",
        "bifi_buyback",
        "chains",
        "price_oracles",
        "vault_ids",
        "vault_strategies",
    ]

    source = sql_database(
        dlt.secrets["source.beefy_db.credentials"]["url"],
        backend="pyarrow",
        chunk_size=1_000_000,
        backend_kwargs={"tz": "UTC"},
        reflection_level="full_with_precision",
    ).with_resources(*tables).parallelize()

    return source
