#!/bin/bash
# Wrapper script for running dlt commands via cron
# Ensures env vars are set and packages are installed

set -euo pipefail

cd /app/dlt

echo "$(date -Iseconds) [run_dlt] Running: ./run.py $*"
exec uv run ./run.py "$@"
