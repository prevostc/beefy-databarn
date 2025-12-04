"""Async pipeline runner with signal handling support."""
from __future__ import annotations
import logging
import asyncio
from typing import Any, Callable, Awaitable
from dataclasses import dataclass
import dlt
from dlt.common.runtime import signals
from lib.config import PIPELINE_ITERATION_TIMEOUT

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


class AsyncPipelineRunner:
    """Handles async execution of multiple pipeline tasks with signal handling."""
    
    def _run_pipeline(self, pipeline: dlt.Pipeline, source: Any, run_mode: str) -> Any:
        """Run a dlt pipeline with an already-instantiated source.
        
        Python does not let you use generators across threads.
        
        Args:
            pipeline: The dlt.Pipeline instance to run
            source: The instantiated source to run
            run_mode: Either "once" or "loop" to control execution mode
            
        Returns:
            The result from pipeline.run()
        """
        logger.info("Running the %s pipeline", pipeline.pipeline_name)

        if run_mode == "once":
            result = pipeline.run(source)
        elif run_mode == "loop":
            iteration_count = 0
            while True:
                iteration_count += 1
                result = pipeline.run(source)
                if result.is_empty:
                    logger.info("%s loop completed (reached end of data after %d iterations)", pipeline.pipeline_name, iteration_count)
                    break
                logger.info("%s loop iteration %d completed", pipeline.pipeline_name, iteration_count)
        else:
            raise ValueError(f"Invalid run_mode: {run_mode}")

        logger.info("%s completed", pipeline.pipeline_name)
        
        return result
    
    async def run_async(self, tasks: list[PipelineTask], resource_filter: dict[str, str] | None = None) -> list[Any]:
        """Run all pipeline tasks in parallel with timeout.
        
        Args:
            tasks: List of PipelineTask instances to execute
            resource_filter: Optional dict mapping pipeline names to resource names to filter.
                           When provided, there should only be one pipeline task.
            
        Returns:
            List of results from pipeline runs
            
        Raises:
            Exception: If any pipeline execution fails (except signal interruptions)
            TimeoutError: If execution exceeds PIPELINE_ITERATION_TIMEOUT
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
        sources = []
        for task in tasks:
            source = await task.get_source()
            # Filter to specific resource if requested for this pipeline
            if resource_filter and task.pipeline.pipeline_name in resource_filter:
                resource_name = resource_filter[task.pipeline.pipeline_name]
                source = source.with_resources(resource_name)
            sources.append((task.pipeline, source, task.run_mode))
        
        logger.info("Running pipelines with timeout: %d seconds", PIPELINE_ITERATION_TIMEOUT)
        async with asyncio.timeout(PIPELINE_ITERATION_TIMEOUT):
            run_calls = [
                asyncio.to_thread(self._run_pipeline, pipeline, source, run_mode)
                for pipeline, source, run_mode in sources
            ]
            results = await asyncio.gather(*run_calls, return_exceptions=True)
        
        # Check for exceptions in results
        for result in results:
            if isinstance(result, BaseException):
                raise result
        
        return results
    
    def run(self, tasks: list[PipelineTask], resource_filter: dict[str, str] | None = None) -> None:
        """Run all pipeline tasks with signal handling.
        
        This is the main entry point that sets up signal handling and runs
        all tasks asynchronously.
        
        Args:
            tasks: List of PipelineTask instances to execute
            resource_filter: Optional dict mapping pipeline names to resource names to filter.
                           If provided, only the specified resource will be run for each pipeline.
                           Example: {"beefy_db_configs": "feebatch_harvests"}
        """
        with signals.intercepted_signals():
            asyncio.run(self.run_async(tasks, resource_filter))
        logger.info("All pipelines completed successfully")

