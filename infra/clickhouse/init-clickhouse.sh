#!/bin/bash
set -e

# Remove empty cert files to prevent permission errors
rm -rf /root/.postgresql/postgresql.crt
rm -rf /root/.postgresql/postgresql.key
wget -O /etc/postgres-ca/root.crt https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

# Initialize ClickHouse schemas and prepare for external table definitions
# This script is automatically executed on first startup via /docker-entrypoint-initdb.d/

echo "Initializing ClickHouse database..."

# Create analytics database
clickhouse-client --password="$CLICKHOUSE_PASSWORD" --query="CREATE DATABASE IF NOT EXISTS analytics"

echo "✓ Created analytics database"

# Note: External tables will be created by dbt staging models
# This script just prepares the schema structure

echo "✓ ClickHouse initialization complete"
echo "  Next: Run 'dbt run' to create external tables and models"

