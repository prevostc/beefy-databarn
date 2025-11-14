#!/bin/bash
set -e

# Find and run the Superset entrypoint
# Try common locations for the Superset entrypoint
if [ -f "/app/docker-entrypoint.sh" ]; then
    ENTRYPOINT="/app/docker-entrypoint.sh"
elif [ -f "/docker-entrypoint.sh" ]; then
    ENTRYPOINT="/docker-entrypoint.sh"
elif [ -f "/app/docker/entrypoints/run-server.sh" ]; then
    ENTRYPOINT="/app/docker/entrypoints/run-server.sh"
else
    echo "No Superset entrypoint found"
    exit 1
fi

# If no arguments provided, default to running Superset
if [ "$#" -eq 0 ]; then
    set -- "run"
fi

# Run the Superset entrypoint in the background
$ENTRYPOINT "$@" &
SUPERSET_PID=$!

# Run the initialization script (creates admin user and adds ClickHouse connection)
# The Python scripts handle retries internally until Superset is ready
/usr/local/bin/provision/provision.sh || echo "Warning: Superset initialization failed, but continuing..."

# Wait for the main process
wait $SUPERSET_PID
