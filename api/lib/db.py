"""ClickHouse database connection and query helpers."""
import logging
from typing import Any, List, Dict, Optional

import clickhouse_connect
from clickhouse_connect.driver import Client

from api.lib.config import settings

logger = logging.getLogger(__name__)

_client: Optional[Client] = None


def get_client() -> Client:
    """Get or create ClickHouse client connection."""
    global _client
    if _client is None:
        try:
            _client = clickhouse_connect.get_client(
                host=settings.CLICKHOUSE_HOST,
                port=settings.CLICKHOUSE_PORT,
                username=settings.CLICKHOUSE_USER,
                password=settings.CLICKHOUSE_PASSWORD,
                database=settings.CLICKHOUSE_DB,
            )
            logger.info(
                f"Connected to ClickHouse at {settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    return _client


def execute_query(query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Execute a query and return results as a list of dictionaries.

    Args:
        query: SQL query string
        parameters: Optional query parameters

    Returns:
        List of dictionaries representing rows

    Raises:
        Exception: If query execution fails
    """
    try:
        client = get_client()
        result = client.query(query, parameters=parameters)
        # Convert result to list of dictionaries
        columns = result.column_names
        rows = []
        for row in result.result_rows:
            rows.append(dict(zip(columns, row)))
        return rows
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        logger.error(f"Query: {query}")
        raise


def close_connection():
    """Close the ClickHouse connection."""
    global _client
    if _client is not None:
        _client.close()
        _client = None
        logger.info("ClickHouse connection closed")

