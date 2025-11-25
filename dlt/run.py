from __future__ import annotations
import logging
import dlt
from lib.config import configure_clickhouse_destination, configure_beefy_db_source, configure_minio_filesystem_destination
from lib.async_runner import AsyncPipelineRunner, PipelineTask
from sources.github_files import github_files
from sources.beefy_api_configs import beefy_api_configs
from sources.beefy_api_snapshots import beefy_api_snapshots
from sources.beefy_db import beefy_db_configs, beefy_db_incremental


if __name__ == "__main__":
    # Configure dlt from environment variables before creating pipelines
    configure_minio_filesystem_destination()
    configure_beefy_db_source()
    configure_clickhouse_destination()
    
    logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
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
            pipeline=dlt.pipeline("github_files", dataset_name="github_files", **pipeline_args),
            get_source=github_files,
            run_mode="once",
        ),
        PipelineTask(
            pipeline=dlt.pipeline("beefy_api_configs", dataset_name="beefy_api_configs", **pipeline_args),
            get_source=beefy_api_configs,
            run_mode="once",
        ),
        PipelineTask(
            pipeline=dlt.pipeline("beefy_api_snapshots", dataset_name="beefy_api_snapshots", **pipeline_args),
            get_source=beefy_api_snapshots,
            run_mode="once",
        ),
        PipelineTask(
            pipeline=dlt.pipeline("beefy_db_configs", dataset_name="beefy_db_configs", **pipeline_args),
            get_source=beefy_db_configs,
            run_mode="once",
        ),
        PipelineTask(
            pipeline=dlt.pipeline("beefy_db_incremental", dataset_name="beefy_db_incremental", **pipeline_args),
            get_source=beefy_db_incremental,
            run_mode="loop",
        ),
    ])

