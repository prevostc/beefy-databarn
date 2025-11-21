from __future__ import annotations
import logging
import dlt
from lib.config import configure_clickhouse_destination, configure_beefy_db_source, configure_minio_filesystem_destination
from lib.async_runner import AsyncPipelineRunner, PipelineTask
from sources.beefy_config_api import beefy_config_api
from sources.beefy_stats_api import beefy_stats_api
from sources.beefy_db import beefy_db_configs, beefy_db_incremental, DATASET_NAME as DB_DATASET_NAME

logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


if __name__ == "__main__":
    # Configure dlt from environment variables before creating pipelines
    configure_minio_filesystem_destination()
    configure_beefy_db_source()
    configure_clickhouse_destination()
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('dlt')
    logger.setLevel(logging.INFO)
    logger = logging.getLogger('dlt.source.sql')
    logger.setLevel(logging.INFO)
    logger = logging.getLogger('dlt.destination.clickhouse')
    logger.setLevel(logging.INFO)
    logger = logging.getLogger('sqlalchemy.engine')
    logger.setLevel(logging.INFO)

    # Pipeline configuration
    pipeline_args = {
        "dev_mode": False,  # otherwise we have dates in the table names
        "progress": "log",
        "destination": "clickhouse",
    }

    # Run all tasks
    runner = AsyncPipelineRunner()
    runner.run([
        PipelineTask(
            pipeline=dlt.pipeline("beefy_config_api", dataset_name="beefy_api", **pipeline_args),
            get_source=beefy_config_api,
            run_mode="once",
        ),
        PipelineTask(
            pipeline=dlt.pipeline("beefy_stats_api", dataset_name="beefy_api", **pipeline_args),
            get_source=beefy_stats_api,
            run_mode="once",
        ),
        PipelineTask(
            pipeline=dlt.pipeline("beefy_db_configs", dataset_name=DB_DATASET_NAME, **pipeline_args),
            get_source=beefy_db_configs,
            run_mode="once",
        ),
        PipelineTask(
            pipeline=dlt.pipeline("beefy_db_incremental", dataset_name=DB_DATASET_NAME, **pipeline_args),
            get_source=beefy_db_incremental,
            run_mode="loop",
        ),
    ])

