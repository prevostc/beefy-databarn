#!/bin/bash
# Wrapper script for running dlt commands via cron
# Ensures env vars are set and packages are installed

set -euo pipefail

# Set PATH to include common binary locations (cron doesn't inherit PATH)
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

cd /app/dlt

echo "$(date -Iseconds) [run_dlt] Running: ./run.py $*"
exec uv run ./run.py "$@"
