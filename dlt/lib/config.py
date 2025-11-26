import os
import dlt

BATCH_SIZE = 1_000_000

os.environ['EXTRACT__WORKERS'] = "3"
os.environ['EXTRACT__DATA_WRITER__DISABLE_COMPRESSION'] = 'true'
os.environ['EXTRACT__DATA_WRITER__BUFFER_MAX_ITEMS'] = str(BATCH_SIZE)
os.environ['EXTRACT__DATA_WRITER__FILE_MAX_ITEMS'] = str(BATCH_SIZE)
os.environ['NORMALIZE__WORKERS'] = '3'
os.environ['NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION'] = 'true'
os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = str(BATCH_SIZE)
os.environ['NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS'] = str(BATCH_SIZE)
os.environ["LOAD__WORKERS"] = "3"
os.environ['LOAD__DATA_WRITER__DISABLE_COMPRESSION'] = 'true'
os.environ['LOAD__DATA_WRITER__BUFFER_MAX_ITEMS'] = str(BATCH_SIZE)
os.environ['LOAD__DATA_WRITER__FILE_MAX_ITEMS'] = str(BATCH_SIZE)


def is_production() -> bool:
    return os.environ.get("DLT_ENV") == "production"


def configure_dlt() -> None:
    """Configure dlt from environment variables."""
    # Set runtime configuration via environment variables
    if "RUNTIME__LOG_LEVEL" in os.environ:
        dlt.config["runtime.log_level"] = os.environ["RUNTIME__LOG_LEVEL"]

    dlt.config["truncate_staging_dataset"] = True



def configure_minio_filesystem_destination() -> None:
    """
    [destination.filesystem]
    bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

    [destination.filesystem.credentials]
    aws_access_key_id = "please set me up!" # copy the access key here
    aws_secret_access_key = "please set me up!" # copy the secret access key here
    """

    # only use minio in prod because dlt connects to the minio server directly
    # but sends connection info to clickhouse to make it read from the minio server directly
    # and in dev clickhouse knows minio via "minio" network alias and dlt as localhost bind

    if is_production():
        minio_bucket_name = os.environ.get("MINIO_DLT_STAGING_BUCKET")
        minio_access_key_id = os.environ.get("MINIO_ACCESS_KEY")
        minio_secret_access_key = os.environ.get("MINIO_SECRET_KEY")
        minio_endpoint = os.environ.get("MINIO_ENDPOINT")

        if not all([minio_bucket_name, minio_access_key_id, minio_secret_access_key, minio_endpoint]):
            raise ValueError("All MinIO configuration must be set")

        dlt.config["destination.filesystem"] = {
            "bucket_url": f"s3://{minio_bucket_name}",
        }

        dlt.secrets["destination.filesystem.credentials"] = {
            "aws_access_key_id": minio_access_key_id,
            "aws_secret_access_key": minio_secret_access_key,
            "endpoint_url": minio_endpoint,
        }
    else:
        dst_dir = os.environ.get("STORAGE_DIR") + "/dlt"
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
            
        dlt.config["destination.filesystem"] = {
            "bucket_url": f"file://{dst_dir}",
        }


def configure_beefy_db_source() -> None:
    """Configure dlt from environment variables."""
    # Set runtime configuration via environment variables
    if "RUNTIME__LOG_LEVEL" in os.environ:
        dlt.config["runtime.log_level"] = os.environ["RUNTIME__LOG_LEVEL"]

    # Configure Beefy DB source credentials from environment variables
    beefy_db_host = os.environ.get("BEEFY_DB_HOST")
    beefy_db_port = os.environ.get("BEEFY_DB_PORT")
    beefy_db_name = os.environ.get("BEEFY_DB_NAME")
    beefy_db_user = os.environ.get("BEEFY_DB_USER")
    beefy_db_password = os.environ.get("BEEFY_DB_PASSWORD")
    beefy_db_sslmode = os.environ.get("BEEFY_DB_SSLMODE") or "require"

    if not all([beefy_db_host, beefy_db_port, beefy_db_name, beefy_db_user, beefy_db_password]):
        raise ValueError("All Beefy DB credentials must be set")

    if beefy_db_host:
        dlt.secrets["source.beefy_db.credentials"] = f"postgresql://{beefy_db_user}:{beefy_db_password}@{beefy_db_host}:{beefy_db_port}/{beefy_db_name}?sslmode={beefy_db_sslmode}"


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
    clickhouse_secure = os.environ.get("DLT_CLICKHOUSE_SECURE", "0")

    if clickhouse_user != "dlt":
        raise ValueError("ClickHouse user must be 'dlt'")

    # Validate all required fields are present
    if not all([clickhouse_host, clickhouse_user, clickhouse_password, clickhouse_database]):
        raise ValueError("All ClickHouse credentials must be set (host, user, password, database)")

    dlt.secrets["destination.clickhouse.credentials"] = f"clickhouse://{clickhouse_user}:{clickhouse_password}@{clickhouse_host}:{clickhouse_port}/{clickhouse_database}?secure={clickhouse_secure}"
    


def get_beefy_db_url() -> str:
    return dlt.secrets["source.beefy_db.credentials"]


def get_clickhouse_credentials() -> str:
    return {
        "host": clickhouse_host,
        "port": clickhouse_port,
        "user": clickhouse_user,
        "password": clickhouse_password,
        "database": clickhouse_database,
        "secure": clickhouse_secure,
    }