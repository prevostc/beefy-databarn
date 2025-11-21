"""Async pipeline runner with signal handling support."""
from __future__ import annotations
import logging
import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Awaitable
from dataclasses import dataclass
import dlt
from dlt.common.runtime import signals
from dlt.common.exceptions import SignalReceivedException
from dlt.pipeline.exceptions import PipelineStepFailed

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
    
    def _run_pipeline(self, pipeline: dlt.Pipeline, source: Any, run_mode: str) -> Any:
        """Run a dlt pipeline in a thread with an already-instantiated source.
        
        Python does not let you use generators across threads.
        
        Args:
            pipeline: The dlt.Pipeline instance to run
            source: The instantiated source to run
            run_mode: Either "once" or "loop" to control execution mode
            
        Returns:
            The result from pipeline.run()
            
        Raises:
            SignalReceivedException: If interrupted by user signal
            KeyboardInterrupt: If interrupted by keyboard
            PipelineStepFailed: If pipeline execution fails
        """
        try:
            logger.info("Running the %s pipeline.", pipeline.pipeline_name)
            if run_mode == "once":
                result = pipeline.run(source)
            elif run_mode == "loop":
                while not (result := pipeline.run(source)).is_empty:
                    logger.info("%s loop completed", pipeline.pipeline_name)
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
    
    async def _run_async(self, tasks: list[PipelineTask]) -> list[Any]:
        """Run all pipeline tasks in parallel using ThreadPoolExecutor.
        
        Args:
            tasks: List of PipelineTask instances to execute
            
        Returns:
            List of results from pipeline runs
            
        Raises:
            Exception: If any pipeline execution fails (except signal interruptions)
        """
        loop = asyncio.get_running_loop()
        
        # Instantiate all async sources
        sources = []
        for task in tasks:
            source = await task.get_source()
            sources.append((task.pipeline, source, task.run_mode))
        
        # Run pipelines in parallel using ThreadPoolExecutor
        try:
            with ThreadPoolExecutor() as executor:
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
    
    def run(self, tasks: list[PipelineTask]) -> None:
        """Run all pipeline tasks with signal handling.
        
        This is the main entry point that sets up signal handling and runs
        all tasks asynchronously.
        
        Args:
            tasks: List of PipelineTask instances to execute
        """
        try:
            with signals.intercepted_signals():
                asyncio.run(self._run_async(tasks))
        except (SignalReceivedException, KeyboardInterrupt):
            logger.info("Shutdown complete.")
            sys.exit(0)

