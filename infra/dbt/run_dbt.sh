#!/bin/bash
# Wrapper script for running dbt commands via cron
# This ensures environment variables are properly set

cd /app/dbt

export DBT_PROFILES_DIR=/app/dbt
export DBT_PROJECT_DIR=/app/dbt

# Run dbt command passed as arguments
exec uv run dbt "$@"

