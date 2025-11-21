from __future__ import annotations
import logging
import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable
import dlt
from dlt.common.runtime import signals
from dlt.common.exceptions import SignalReceivedException
from dlt.pipeline.exceptions import PipelineStepFailed
from lib.config import configure_clickhouse_destination, configure_beefy_db_source, configure_minio_filesystem_destination, BATCH_SIZE
from sources.beefy_config_api import beefy_config_api
from sources.beefy_stats_api import beefy_stats_api
from sources.beefy_db import beefy_db_configs, DATASET_NAME as DB_DATASET_NAME

logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


def _run_pipeline(pipeline: dlt.Pipeline, source: Any, write_disposition: str = "merge", mode: str = "full") -> Any:
    """Run a dlt pipeline in a thread with an already-instantiated source.
    
    Python does not let you use generators across threads.
    """
    try:
        logger.info(f"Running the %s pipeline.", pipeline.pipeline_name)
        if mode == "full":
            pipeline.run(source)
        elif mode == "incremental":
            while True:
                logger.info(f"Running the {pipeline.pipeline_name} pipeline in full mode.")
                rows_to_process = BATCH_SIZE * 20
                pipeline.run(source.add_limit(max_items=rows_to_process, count_rows=True))
                row_counts = pipeline.last_trace.last_normalize_info.row_counts
                if row_counts["total_rows"] < rows_to_process:
                    break
        else:
            raise ValueError(f"Invalid mode: {mode}")
        logger.info("%s completed.", pipeline.pipeline_name)
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


async def _run_async(pipelines_to_run: list[tuple[dlt.Pipeline, Callable, str, str]]):
    """Run all pipelines in parallel using ThreadPoolExecutor."""
    loop = asyncio.get_running_loop()
    
    # Instantiate all async sources
    sources = []
    for pipeline, get_source, write_disposition, mode in pipelines_to_run:
        source = await get_source()
        sources.append((pipeline, source, write_disposition, mode))
    
    # Run pipelines in parallel using ThreadPoolExecutor
    try:
        with ThreadPoolExecutor() as executor:
            tasks = [
                loop.run_in_executor(
                    executor,
                    _run_pipeline,
                    pipeline,
                    source,
                    write_disposition,
                    mode,
                )
                for pipeline, source, write_disposition, mode in sources
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check for exceptions in results
        for (pipeline, _, _, _), result in zip(sources, results):
            if isinstance(result, (SignalReceivedException, KeyboardInterrupt)):
                logger.info("Pipeline execution interrupted by user signal.")
                return
            elif isinstance(result, Exception):
                raise result
            else:
                logger.info("%s load: %s", pipeline.pipeline_name, result)
                print(f"{pipeline.pipeline_name} load:", result)
        
        return results
    except (SignalReceivedException, KeyboardInterrupt):
        logger.info("Pipeline execution interrupted by user signal.")
        return


if __name__ == "__main__":
    # Configure dlt from environment variables before creating pipelines
    configure_minio_filesystem_destination()
    configure_beefy_db_source()
    configure_clickhouse_destination()
    
    logging.basicConfig(level=logging.INFO)

    args = {
        "dev_mode": False, # otherwise we have dates in the table names
        "progress": "log",
        "destination": "clickhouse",
        "staging": "filesystem",
    }

    pipelines_to_run = [
        (dlt.pipeline("beefy_config_api", dataset_name="beefy_api", **args), beefy_config_api, "merge", "full"),
        (dlt.pipeline("beefy_stats_api", dataset_name="beefy_api", **args), beefy_stats_api, "merge", "full"),
        (dlt.pipeline("beefy_db_configs", dataset_name=DB_DATASET_NAME, **args), beefy_db_configs, "replace", "full"),
    ]
    
    try:
        with signals.intercepted_signals():
            asyncio.run(_run_async(pipelines_to_run))
    except (SignalReceivedException, KeyboardInterrupt):
        logger.info("Shutdown complete.")
        sys.exit(0)
    

