from __future__ import annotations
import logging
from typing import Any
import dlt
from lib.clickhouse import configure_clickhouse_destination
from sources.beefy_config_api import beefy_config_api
from sources.beefy_stats_api import beefy_stats_api

logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


def run_beefy_config_api() -> Any:
    """Run the Beefy Config API pipeline."""
    logger.info("Running the Beefy API pipeline.")
    pipeline = dlt.pipeline(
        pipeline_name="beefy_config_api",
        destination="clickhouse",
        dataset_name="beefy_config_api",
    )

    load = pipeline.run(beefy_config_api())
    logger.info("Beefy Config API pipeline completed.")
    return load



def run_beefy_stats_api() -> Any:
    """Run the Beefy Stats API pipeline."""
    logger.info("Running the Beefy Stats API pipeline.")
    pipeline = dlt.pipeline(
        pipeline_name="beefy_stats_api",
        destination="clickhouse",
        dataset_name="beefy_stats_api",
    )
    load = pipeline.run(beefy_stats_api())
    logger.info("Beefy Stats API pipeline completed.")
    return load

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Configure dlt from environment variables before creating pipeline
    configure_clickhouse_destination()

    config_load = run_beefy_config_api()
    print("Config API load:", config_load)

    stats_load = run_beefy_stats_api()
    print("Stats API load:", stats_load)
    

