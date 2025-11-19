import os
import dlt

def configure_clickhouse_destination() -> None:
    """Configure dlt from environment variables."""
    # Set runtime configuration via environment variables
    if "RUNTIME__LOG_LEVEL" in os.environ:
        dlt.config["runtime.log_level"] = os.environ["RUNTIME__LOG_LEVEL"]
    
    # Configure ClickHouse destination credentials from environment variables
    clickhouse_host = os.environ.get("DLT_CLICKHOUSE_HOST")
    clickhouse_user = os.environ.get("DLT_CLICKHOUSE_USER")
    clickhouse_password = os.environ.get("DLT_CLICKHOUSE_PASSWORD")
    clickhouse_database = os.environ.get("DLT_CLICKHOUSE_DB")
    clickhouse_port = int(os.environ.get("DLT_CLICKHOUSE_PORT", "9000"))
    clickhouse_http_port = int(os.environ.get("DLT_CLICKHOUSE_HTTP_PORT", "8123"))

    if clickhouse_user != "dlt":
        raise ValueError("ClickHouse user must be 'dlt'")

    if clickhouse_host:
        dlt.secrets["destination.clickhouse.credentials"] = {
            "host": clickhouse_host,
            "port": clickhouse_port,
            "user": clickhouse_user,
            "password": clickhouse_password,
            "database": clickhouse_database,
            "http_port": clickhouse_http_port,
            "secure": 0,
        }

