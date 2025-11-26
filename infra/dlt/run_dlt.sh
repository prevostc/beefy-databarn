#!/bin/bash
# Wrapper script for running dlt commands via cron
# Ensures env vars are set and packages are installed

set -euo pipefail

# Set PATH to include common binary locations (cron doesn't inherit PATH)
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

# Source environment variables exported by entrypoint script
if [ -f /app/infra/dlt/cron-env ]; then
    set -a
    source /app/infra/dlt/cron-env
    set +a
fi

cd /app/dlt

echo "$(date -Iseconds) [run_dlt] Running: ./run.py $*"
exec uv run ./run.py "$@"
