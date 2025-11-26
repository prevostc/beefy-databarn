#!/bin/bash
# Entrypoint script for DLT container
# Exports environment variables for cron jobs

set -euo pipefail

# Ensure directory exists
mkdir -p /app/infra/dlt

# Create environment file from current environment variables
# This file will be sourced by cron jobs
env | grep -E '^(STORAGE_DIR|DLT_|BEEFY_DB_|MINIO_)=' > /app/infra/dlt/cron-env || true

# Start cron in foreground
exec cron -f

