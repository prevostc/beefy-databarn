import logging
from typing import Any
from datetime import datetime, timedelta, timezone
import dlt
import psycopg2
import sqlalchemy as sa
from sqlalchemy import text, MetaData
from dlt.sources.sql_database import sql_table, sql_database
from dlt.sources.sql_database.helpers import SelectClause, Table
from dlt.destinations.impl.clickhouse.typing import TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR
from lib.config import BATCH_SIZE, get_beefy_db_url
import os

logger = logging.getLogger(__name__)

DATE_RANGE_SIZE_IN_DAYS = 120


def _fetch_cluster_keys() -> tuple[list[int], list[int]]:
    """Fetch cluster keys from the database."""
    db_url = get_beefy_db_url()
    
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

    db_url = get_beefy_db_url()
    chain_ids, oracle_ids = _fetch_cluster_keys()
    
    # # Harvests table 
    def harvests_query_adapter_callback(query, table, incremental=None, engine=None):
        start_value = incremental.start_value
        if start_value is None:
            start_value = datetime(2022, 1, 13, 0, 0, 0, tzinfo=timezone.utc) #  2022-01-13 08:32:56+00

        end_value = start_value + timedelta(days=DATE_RANGE_SIZE_IN_DAYS)

        logger.info(f"harvests_query_adapter_callback: {start_value} {end_value}")

        return sa.text(
            f"SELECT "
            f"   *"
            f" FROM {table.fullname} "
            f" WHERE chain_id = ANY(:chain_ids) "
            f" AND txn_timestamp > :start_value "
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
        write_disposition="append",
        incremental=dlt.sources.incremental(
            "txn_timestamp", 
            initial_value=None,
            primary_key=["chain_id", "block_number", "txn_idx", "event_idx"],
            last_value_func=max,
            row_order="asc" 
        ),
    )
    harvests.apply_hints(
        columns=[
            # force keys to be non-nullable
            {"name": "chain_id", "nullable": False },
            {"name": "block_number", "nullable": False },
            {"name": "txn_idx", "nullable": False },
            {"name": "event_idx", "nullable": False },
            # make sure metrics have enough precision, Decimal256(20) -> Decimal(76, 20)
            {"name": "call_fee", "data_type": "decimal", "scale": 20, "precision": 76 },
            {"name": "gas_fee", "data_type": "decimal", "scale": 20, "precision": 76 },
            {"name": "platform_fee", "data_type": "decimal", "scale": 20, "precision": 76 },
            {"name": "strategist_fee", "data_type": "decimal", "scale": 20, "precision": 76 },
            {"name": "harvest_amount", "data_type": "decimal", "scale": 20, "precision": 76 },
            {"name": "native_price", "data_type": "decimal", "scale": 20, "precision": 76 },
            {"name": "want_price", "data_type": "decimal", "scale": 20, "precision": 76 },
        ]
    )


    # # Prices table
    def prices_query_adapter_callback(query, table, incremental=None, engine=None):
        start_value = incremental.start_value
        if start_value is None:
            start_value = datetime(2021, 7, 31, 0, 0, 0, tzinfo=timezone.utc) #  2021-07-31 19:30:00+00

        end_value = start_value + timedelta(days=DATE_RANGE_SIZE_IN_DAYS)

        logger.info(f"prices_query_adapter_callback: {start_value} {end_value}")

        return sa.text(
            f"SELECT * "
            f" FROM {table.fullname} "
            f" WHERE oracle_id = ANY(:oracle_ids) "
            f" AND t > :start_value "
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
        write_disposition="append", 
        incremental=dlt.sources.incremental(
            "t", 
            initial_value=None,
            primary_key=["oracle_id", "t"],
            last_value_func=max,
            row_order="asc" 
        ),
    )
    prices.apply_hints(
        columns=[
            # force keys to be non-nullable
            {"name": "oracle_id", "nullable": False },
            {"name": "t", "nullable": False },

            # make sure metrics have enough precision, Decimal256(20) -> Decimal(76, 20)
            {"name": "val", "data_type": "decimal", "scale": 20, "precision": 256},
        ]
    )

    return ( 
        harvests, 
        prices 
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
