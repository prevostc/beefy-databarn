#!/bin/bash
# Wrapper script for running dbt commands via cron
# Ensures env vars are set and packages are installed

set -euo pipefail

cd /app/dbt
export DBT_PROFILES_DIR=/app/dbt
export DBT_PROJECT_DIR=/app/dbt

# Install dbt packages if needed
if [ ! -d dbt_packages ] || [ -z "$(ls -A dbt_packages)" ]; then
  echo "$(date -Iseconds) [run_dbt] dbt_packages empty, running 'dbt deps'..."
  uv run dbt deps
fi

echo "$(date -Iseconds) [run_dbt] Running: dbt $*"
exec uv run dbt "$@"
