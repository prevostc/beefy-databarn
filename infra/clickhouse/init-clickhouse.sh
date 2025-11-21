#!/bin/bash
set -euo pipefail

echo "Initializing ClickHouse databases..."

clickhouse-client --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --query="CREATE DATABASE IF NOT EXISTS analytics"
clickhouse-client --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --query="CREATE DATABASE IF NOT EXISTS connectors"
clickhouse-client --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --query="CREATE DATABASE IF NOT EXISTS dlt"

echo "✓ Databases analytics & dlt created"
