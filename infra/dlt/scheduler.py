#!/usr/bin/env python3
"""
Scheduler for DLT pipelines using APScheduler.
Runs the DLT pipeline every 5 minutes.
"""
from __future__ import annotations
import logging
import asyncio
import sys
from dlt.common.runtime import signals
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Add dlt directory to path for imports (scheduler is in infra/dlt, dlt code is in /app/dlt)
sys.path.insert(0, "/app/dlt")

# Import the pipeline runner function from run.py
from run import run_pipelines

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def run_dlt_pipeline():
    """Run the DLT pipeline by calling the reusable function from run.py."""
    try:
        logger.info("Starting DLT pipeline run...")
        with signals.intercepted_signals():
            asyncio.run(run_pipelines())
        logger.info("DLT pipeline run completed successfully")
    except Exception as e:
        logger.error(f"Error running DLT pipeline: {e}", exc_info=True)


if __name__ == "__main__":
    logger.info("Starting DLT scheduler (runs every 5 minutes)...")
    

    scheduler = AsyncIOScheduler()

    # Schedule the pipeline to run every 5 minutes
    scheduler.add_job(
        run_dlt_pipeline,
        trigger=CronTrigger(minute="*/5"),
        id="dlt_pipeline",
        name="DLT Pipeline",
        max_instances=1,  # Prevent overlapping runs
        coalesce=True,   # Combine multiple pending runs into one
    )


    scheduler.start()
    asyncio.get_event_loop().run_forever()

