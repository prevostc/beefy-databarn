#!/bin/bash
set -euo pipefail

echo "Initializing ClickHouse databases..."

clickhouse-client --query="CREATE DATABASE IF NOT EXISTS analytics"
clickhouse-client --query="CREATE DATABASE IF NOT EXISTS dlt"

echo "✓ Databases analytics & dlt created"
