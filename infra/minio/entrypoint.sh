#!/bin/bash

set -e

# Start MinIO server in the background
# "$@" contains the command arguments: server /data --console-address ":9001"
/usr/bin/minio "$@" &
MINIO_PID=$!

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  if curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "MinIO is ready!"
    break
  fi
  RETRY_COUNT=$((RETRY_COUNT + 1))
  echo "Waiting for MinIO... (attempt $RETRY_COUNT/$MAX_RETRIES)"
  sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
  echo "Error: MinIO failed to start within expected time"
  exit 1
fi

# Give MinIO a moment to fully initialize
sleep 2

# Create alias and buckets
/usr/bin/mc alias set beefy-databarn http://localhost:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}
/usr/bin/mc mb beefy-databarn/${MINIO_DLT_STAGING_BUCKET} || true  # Ignore error if bucket already exists
/usr/bin/mc policy set public beefy-databarn/${MINIO_DLT_STAGING_BUCKET}

# Wait for MinIO process to keep container running
wait $MINIO_PID
