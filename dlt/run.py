from __future__ import annotations
import logging
import sys
import dlt
from lib.config import configure_dlt, configure_clickhouse_destination, configure_beefy_db_source, configure_minio_filesystem_destination
from lib.async_runner import AsyncPipelineRunner, PipelineTask
from sources.github_files import github_files
from sources.beefy_api_configs import beefy_api_configs
from sources.beefy_api_snapshots import beefy_api_snapshots
from sources.beefy_db import beefy_db_configs, beefy_db_incremental 


def run_pipelines(resource: str | None = None):
    """
    Configure and run DLT pipelines.
    This function can be called directly or imported by schedulers.
    
    Args:
        resource: Optional resource identifier to run.
                 Format: "pipeline_name" or "pipeline_name.resource_name"
                 Examples: "beefy_db_configs" or "beefy_db_configs.feebatch_harvests"
                 If None, runs all pipelines.
                 Valid pipeline names: github_files, beefy_api_configs, beefy_api_snapshots,
                                      beefy_db_configs, beefy_db_incremental
    """
    # Configure dlt from environment variables before creating pipelines
    configure_dlt()
    configure_minio_filesystem_destination()
    configure_beefy_db_source()
    configure_clickhouse_destination()

    # Pipeline configuration
    pipeline_args = {
        "dev_mode": False,  # otherwise we have dates in the table names
        "progress": "log",
        "destination": "clickhouse",
        "staging": "filesystem",
    }

    # Parse resource identifier if provided
    pipeline_name, resource_name = resource.split(".", 1) if resource and "." in resource else (resource, None)
    resource_filter = {pipeline_name: resource_name} if pipeline_name and resource_name else None

    # Define all available pipeline tasks (configuration is always the same)
    all_tasks = [
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
    ]

    # Filter tasks if a specific pipeline name is provided
    tasks = [task for task in all_tasks if not pipeline_name or task.pipeline.pipeline_name == pipeline_name]
    if pipeline_name and not tasks:
        available_names = [task.pipeline.pipeline_name for task in all_tasks]
        logging.error(
            f"Pipeline '{pipeline_name}' not found. Available pipelines: {', '.join(available_names)}"
        )
        sys.exit(1)

    # Run selected tasks with optional resource filtering
    AsyncPipelineRunner().run(tasks, resource_filter=resource_filter)


if __name__ == "__main__":
    # Get resource identifier from command-line argument if provided
    # Format: "pipeline_name" or "pipeline_name.resource_name"
    resource = sys.argv[1] if len(sys.argv) > 1 else None
    run_pipelines(resource)

