import logging
from typing import Any
from datetime import datetime, timedelta, timezone
import dlt
import psycopg2
import sqlalchemy as sa
from sqlalchemy import text, MetaData
from dlt.sources.sql_database import sql_table, sql_database
from dlt.sources.sql_database.helpers import SelectClause, Table
from lib.config import BATCH_SIZE
import os

logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

DATASET_NAME = "beefy_db"

LAG_BUFFER_IN_DAYS = 7
DATE_RANGE_SIZE_IN_DAYS = 120


def _fetch_cluster_keys() -> tuple[list[int], list[int]]:
    """Fetch cluster keys from the database."""
    db_url = dlt.secrets["source.beefy_db.credentials"]["url"]
    
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT chain_id FROM chains ORDER BY chain_id")
            chain_ids = [row[0] for row in cur.fetchall()]
            
            # Fetch oracle_ids (id column) from price_oracles table
            cur.execute("SELECT DISTINCT id FROM price_oracles ORDER BY id")
            oracle_ids = [row[0] for row in cur.fetchall()]
            
            logger.info(f"Fetched {len(chain_ids)} chain_ids and {len(oracle_ids)} oracle_ids")
            return chain_ids, oracle_ids
    finally:
        conn.close()


@dlt.source(name="beefy_db_incremental", parallelized=True)
async def beefy_db_incremental() -> Any:
    """Expose Beefy DB resources for use by dlt pipelines."""

    db_url = dlt.secrets["source.beefy_db.credentials"]["url"]
    chain_ids, oracle_ids = _fetch_cluster_keys()
    
    # # Harvests table 
    def harvests_query_adapter_callback(query, table, incremental=None, engine=None):
        start_value = incremental.start_value
        if start_value is None:
            start_value = datetime(2022, 1, 13, 0, 0, 0, tzinfo=timezone.utc) #  2022-01-13 08:32:56+00

        start_value = start_value - timedelta(days=LAG_BUFFER_IN_DAYS)
        end_value = start_value + timedelta(days=DATE_RANGE_SIZE_IN_DAYS)

        logger.info(f"harvests_query_adapter_callback: {start_value} {end_value}")

        return sa.text(
            f"SELECT "
            f"   *"
            f" FROM {table.fullname} "
            f" WHERE chain_id = ANY(:chain_ids) "
            f" AND txn_timestamp >= :start_value "
            f" AND txn_timestamp <= :end_value "
        ).bindparams(**{
            "start_value": start_value,
            "end_value": end_value,
            "chain_ids": chain_ids,
        })
        
    harvests = sql_table(
        credentials=db_url,
        table="harvests",
        backend="pyarrow",
        chunk_size=BATCH_SIZE,
        backend_kwargs={"tz": "UTC"},
        reflection_level="full_with_precision",
        query_adapter_callback=harvests_query_adapter_callback,
        primary_key=["chain_id", "block_number", "txn_idx", "event_idx"],
        incremental=dlt.sources.incremental(
            "txn_timestamp", 
            initial_value=None,
            primary_key=["chain_id", "block_number", "txn_idx", "event_idx"],
            last_value_func=max,
            row_order="asc" 
        ),
        # clickhouse will dedup on primary key, so we can just append
        # merge won't work because it's not supporting composite primary keys
        # and the dedup process is way too memory intensive to happen in-place anyway
        write_disposition="append",
    )


    # # Prices table
    def prices_query_adapter_callback(query, table, incremental=None, engine=None):
        start_value = incremental.start_value
        if start_value is None:
            start_value = datetime(2021, 7, 31, 0, 0, 0, tzinfo=timezone.utc) #  2021-07-31 19:30:00+00

        start_value = start_value - timedelta(days=LAG_BUFFER_IN_DAYS)
        end_value = start_value + timedelta(days=DATE_RANGE_SIZE_IN_DAYS)

        logger.info(f"prices_query_adapter_callback: {start_value} {end_value}")

        return sa.text(
            f"SELECT * "
            f" FROM {table.fullname} "
            f" WHERE oracle_id = ANY(:oracle_ids) "
            f" AND t >= :start_value "
            f" AND t <= :end_value "
        ).bindparams(**{
            "start_value": start_value,
            "end_value": end_value,
            "oracle_ids": oracle_ids,
        })

    prices = sql_table(
        credentials=db_url,
        table="prices",
        backend="pyarrow",
        chunk_size=BATCH_SIZE,
        backend_kwargs={"tz": "UTC"},
        reflection_level="full_with_precision",
        query_adapter_callback=prices_query_adapter_callback,
        primary_key=["oracle_id", "t"],
        incremental=dlt.sources.incremental(
            "t", 
            initial_value=None,
            primary_key=["oracle_id", "t"],
            last_value_func=max,
            row_order="asc" 
        ),
        # clickhouse will dedup on primary key, so we can just append
        # merge won't work because it's not supporting composite primary keys
        # and the dedup process is way too memory intensive to happen in-place anyway
        write_disposition="append", 
    )
    
    return ( 
        harvests, 
        prices 
    )



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
        chunk_size=BATCH_SIZE,
        backend_kwargs={"tz": "UTC"},
        reflection_level="full_with_precision",
    ).with_resources(*tables).parallelize()

    return source
