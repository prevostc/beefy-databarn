from __future__ import annotations
import logging
from typing import List
import sys
import dlt
from lib.config import configure_dlt, configure_clickhouse_destination, configure_beefy_db_source, configure_minio_filesystem_destination
from lib.async_runner import AsyncPipelineRunner, PipelineTask
from sources.github_files import github_files
from sources.beefy_api_configs import beefy_api_configs
from sources.beefy_api_snapshots import beefy_api_snapshots
from sources.beefy_db import beefy_db_configs, beefy_db_incremental 


def configure_env() -> None:
    """Configure the environment for the DLT pipelines."""
    configure_dlt()
    configure_minio_filesystem_destination()
    configure_beefy_db_source()
    configure_clickhouse_destination()


def get_all_tasks() -> List[PipelineTask]:
    """Get the tasks to run."""

    # Pipeline configuration
    pipeline_args = {
        "dev_mode": False,  # otherwise we have dates in the table names
        "progress": "log",
        "destination": "clickhouse",
        "staging": "filesystem",
    }

    # Define all available pipeline tasks (configuration is always the same)
    return [
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


def get_resource_filter(all_tasks: List[PipelineTask], resource: str | None = None) -> dict[str, str] | None:
    """Get the resource filter for the tasks."""
    # Filter tasks if a specific pipeline name is provided
    pipeline_name, resource_name = resource.split(".", 1) if resource and "." in resource else (resource, None)
    resource_filter = {pipeline_name: resource_name} if pipeline_name and resource_name else None
    tasks = [task for task in all_tasks if not pipeline_name or task.pipeline.pipeline_name == pipeline_name]
    if pipeline_name and not tasks:
        available_names = [task.pipeline.pipeline_name for task in all_tasks]
        logging.error(
            f"Pipeline '{pipeline_name}' not found. Available pipelines: {', '.join(available_names)}"
        )
        sys.exit(1)
    
    return resource_filter


def run_pipelines(resource: str | None = None):
    configure_env()
    tasks = get_all_tasks()
    resource_filter = get_resource_filter(tasks, resource)
    AsyncPipelineRunner().run(tasks, resource_filter=resource_filter)


async def run_pipelines_async(resource: str | None = None):
    configure_env()
    tasks = get_all_tasks()
    resource_filter = get_resource_filter(tasks, resource)
    return await AsyncPipelineRunner().run_async(tasks, resource_filter=resource_filter)




if __name__ == "__main__":
    # Get resource identifier from command-line argument if provided
    # Format: "pipeline_name" or "pipeline_name.resource_name"
    resource = sys.argv[1] if len(sys.argv) > 1 else None
    run_pipelines(resource)

