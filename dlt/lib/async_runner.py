"""Async pipeline runner with signal handling support."""
from __future__ import annotations
import logging
import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Any, Callable, Awaitable
from dataclasses import dataclass
import dlt
from dlt.common.runtime import signals
from dlt.common.exceptions import SignalReceivedException
from dlt.pipeline.exceptions import PipelineStepFailed
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
    
    def __init__(self):
        """Initialize the async pipeline runner."""
        pass
    
    def _run_pipeline_with_timeout(self, pipeline: dlt.Pipeline, source: Any, timeout: int) -> Any:
        """Run a single pipeline.run() call with a timeout.
        
        Args:
            pipeline: The dlt.Pipeline instance to run
            source: The instantiated source to run
            timeout: Timeout in seconds for this iteration
            
        Returns:
            The result from pipeline.run()
            
        Raises:
            FuturesTimeoutError: If the pipeline run exceeds the timeout
            SignalReceivedException: If interrupted by user signal
            KeyboardInterrupt: If interrupted by keyboard
            PipelineStepFailed: If pipeline execution fails
        """
        # Use a separate executor to run the pipeline with timeout
        with ThreadPoolExecutor(max_workers=1) as timeout_executor:
            future = timeout_executor.submit(pipeline.run, source)
            try:
                result = future.result(timeout=timeout)
                return result
            except FuturesTimeoutError:
                logger.error(
                    "%s pipeline iteration timed out after %d seconds. "
                    "This iteration will be skipped.",
                    pipeline.pipeline_name,
                    timeout
                )
                # Cancel the future if possible
                future.cancel()
                raise

    def _run_pipeline(self, pipeline: dlt.Pipeline, source: Any, run_mode: str, timeout: int = PIPELINE_ITERATION_TIMEOUT) -> Any:
        """Run a dlt pipeline in a thread with an already-instantiated source.
        
        Python does not let you use generators across threads.
        
        Args:
            pipeline: The dlt.Pipeline instance to run
            source: The instantiated source to run
            run_mode: Either "once" or "loop" to control execution mode
            timeout: Timeout in seconds for each pipeline iteration (default: from env or 3600)
            
        Returns:
            The result from pipeline.run()
            
        Raises:
            FuturesTimeoutError: If a pipeline iteration exceeds the timeout
            SignalReceivedException: If interrupted by user signal
            KeyboardInterrupt: If interrupted by keyboard
            PipelineStepFailed: If pipeline execution fails
        """
        try:
            logger.info("Running the %s pipeline (timeout: %d seconds per iteration).", pipeline.pipeline_name, timeout)
            if run_mode == "once":
                result = self._run_pipeline_with_timeout(pipeline, source, timeout)
            elif run_mode == "loop":
                iteration_count = 0
                consecutive_timeouts = 0
                max_consecutive_timeouts = 3  # Stop if we timeout 3 times in a row
                
                while True:
                    iteration_count += 1
                    try:
                        result = self._run_pipeline_with_timeout(pipeline, source, timeout)
                        consecutive_timeouts = 0  # Reset timeout counter on success
                        if result.is_empty:
                            logger.info("%s loop completed (reached end of data after %d iterations)", pipeline.pipeline_name, iteration_count)
                            break
                        logger.info("%s loop iteration %d completed", pipeline.pipeline_name, iteration_count)
                    except FuturesTimeoutError:
                        consecutive_timeouts += 1
                        logger.warning(
                            "%s loop iteration %d timed out after %d seconds (consecutive timeouts: %d). "
                            "Continuing to next iteration.",
                            pipeline.pipeline_name,
                            iteration_count,
                            timeout,
                            consecutive_timeouts
                        )
                        # For loop mode, continue to next iteration after timeout
                        # But stop if we timeout too many times in a row (might indicate a stuck pipeline)
                        if consecutive_timeouts >= max_consecutive_timeouts:
                            logger.error(
                                "%s loop stopped after %d consecutive timeouts. "
                                "Pipeline may be stuck. Last successful iteration: %d",
                                pipeline.pipeline_name,
                                consecutive_timeouts,
                                iteration_count - consecutive_timeouts
                            )
                            # Raise a timeout error to stop the loop
                            raise FuturesTimeoutError(
                                f"Pipeline {pipeline.pipeline_name} timed out {consecutive_timeouts} times consecutively"
                            )
                        continue
            else:
                raise ValueError(f"Invalid run_mode: {run_mode}")
            logger.info("%s completed", pipeline.pipeline_name)
            return result
        except (SignalReceivedException, KeyboardInterrupt) as e:
            logger.info("%s interrupted by user signal.", pipeline.pipeline_name)
            raise
        except PipelineStepFailed as e:
            # Check if the underlying exception is a SignalReceivedException
            if isinstance(e.__cause__, SignalReceivedException):
                logger.info("%s interrupted by user signal.", pipeline.pipeline_name)
                # Re-raise the original SignalReceivedException
                raise e.__cause__
            # Otherwise, re-raise the original exception
            raise
    
    async def _run_async(self, tasks: list[PipelineTask], resource_filter: dict[str, str] | None = None) -> list[Any]:
        """Run all pipeline tasks in parallel using ThreadPoolExecutor.
        
        Args:
            tasks: List of PipelineTask instances to execute
            resource_filter: Optional dict mapping pipeline names to resource names to filter.
                           When provided, there should only be one pipeline task.
            
        Returns:
            List of results from pipeline runs
            
        Raises:
            Exception: If any pipeline execution fails (except signal interruptions)
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
        
        # Instantiate and filter all sources BEFORE starting any threads/processes
        sources = []
        for task in tasks:
            source = await task.get_source()
            # Filter to specific resource if requested for this pipeline
            if resource_filter and task.pipeline.pipeline_name in resource_filter:
                resource_name = resource_filter[task.pipeline.pipeline_name]
                source = source.with_resources(resource_name)
            sources.append((task.pipeline, source, task.run_mode))
        
        # Now that all sources are instantiated and filtered, run pipelines in parallel
        loop = asyncio.get_running_loop()
        try:
            executor = ThreadPoolExecutor()
            try:
                futures = [
                    loop.run_in_executor(
                        executor,
                        self._run_pipeline,
                        pipeline,
                        source,
                        run_mode,
                    )
                    for pipeline, source, run_mode in sources
                ]
                results = await asyncio.gather(*futures, return_exceptions=True)
            finally:
                # Explicitly shutdown executor to ensure all threads are cleaned up
                executor.shutdown(wait=True)
            
            # Check for exceptions in results
            for (pipeline, _, _), result in zip(sources, results):
                if isinstance(result, (SignalReceivedException, KeyboardInterrupt)):
                    logger.info("Pipeline execution interrupted by user signal.")
                    return []
                elif isinstance(result, Exception):
                    raise result
                else:
                    logger.info("%s load: %s", pipeline.pipeline_name, result)
                    print(f"{pipeline.pipeline_name} load:", result)
            
            return results
        except (SignalReceivedException, KeyboardInterrupt):
            logger.info("Pipeline execution interrupted by user signal.")
            return []
    
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
        try:
            with signals.intercepted_signals():
                asyncio.run(self._run_async(tasks, resource_filter))
            # Ensure we log completion
            logger.info("All pipelines completed successfully")
        except (SignalReceivedException, KeyboardInterrupt):
            logger.info("Shutdown complete.")
            sys.exit(0)

