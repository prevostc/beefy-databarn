"""Async pipeline runner with signal handling support."""
from __future__ import annotations
import logging
import asyncio
from typing import Any, Callable, Awaitable
from dataclasses import dataclass
import dlt
from dlt.common.runtime import signals
from lib.config import is_production

logger = logging.getLogger(__name__)


@dataclass
class PipelineTask:
    """Represents a pipeline task to be executed.
    
    Attributes:
        pipeline: The dlt.Pipeline instance
        get_source: Async callable that returns the source to run
        run_mode: Either "once" or "loop" to control execution mode
    """
    pipeline: dlt.Pipeline
    get_source: Callable[[], Awaitable[Any]]
    run_mode: str = "once"
    
    def __post_init__(self):
        """Validate run_mode."""
        if self.run_mode not in ("once", "loop"):
            raise ValueError(f"run_mode must be 'once' or 'loop', got '{self.run_mode}'")


class PipelineRunner:
    """Handles async execution of multiple pipeline tasks with signal handling."""
    
    async def run(self, tasks: list[PipelineTask], resource_filter: dict[str, str] | None = None) -> None:
        """Run all pipeline tasks asynchronously with signal handling.
        
        This is the main entry point that sets up signal handling and runs
        all tasks asynchronously.
        
        Args:
            tasks: List of PipelineTask instances to execute
            resource_filter: Optional dict mapping pipeline names to resource names to filter.
                           If provided, only the specified resource will be run for each pipeline.
                           Example: {"beefy_db_configs": "feebatch_harvests"}
        """


        # When filtering to a specific resource, there should only be one pipeline task
        if resource_filter:
            if len(tasks) != 1:
                raise ValueError(
                    f"Expected exactly one pipeline task when resource_filter is provided, "
                    f"but got {len(tasks)} tasks. Resource filter: {resource_filter}"
                )
            if list(resource_filter.keys())[0] != tasks[0].pipeline.pipeline_name:
                raise ValueError(
                    f"Resource filter pipeline name '{list(resource_filter.keys())[0]}' "
                    f"does not match task pipeline name '{tasks[0].pipeline.pipeline_name}'"
                )
        
        # Instantiate all sources upfront before threading

        exceptions = []
        
        for task in tasks:
            try:
                source = await task.get_source()
                # Filter to specific resource if requested for this pipeline
                if resource_filter and task.pipeline.pipeline_name in resource_filter:
                    resource_name = resource_filter[task.pipeline.pipeline_name]
                    source = source.with_resources(resource_name)

                logger.info("Running pipeline %s", task.pipeline.pipeline_name)
                if task.run_mode == "once":
                    result = task.pipeline.run(source)
                elif task.run_mode == "loop":
                    iteration_count = 0
                    while True:
                        iteration_count += 1
                        result = task.pipeline.run(source)
                        if result.is_empty:
                            logger.info("%s loop completed (reached end of data after %d iterations)", task.pipeline.pipeline_name, iteration_count)
                            break
                        logger.info("%s loop iteration %d completed", task.pipeline.pipeline_name, iteration_count)
                else:
                    raise ValueError(f"Invalid run_mode: {task.run_mode}")

                logger.info("%s completed", task.pipeline.pipeline_name)
            except Exception as e:
                logger.error("Error occurred while running pipeline %s: %s", task.pipeline.pipeline_name, e)
                if not is_production():
                    raise
                exceptions.append(e)
        if exceptions:
            raise PipelineRunnerError(exceptions)

        logger.info("All pipelines completed successfully")

  
class PipelineRunnerError(Exception):
    """Exception raised when an error occurs while running a pipeline."""
    
    def __init__(self, causes: list[Exception]):
        self.causes = causes
        super().__init__(f"Errors occurred while running pipelines: {causes}")