#!/bin/bash
set -e

# Initialize Postgres databases for Superset and Grafana
# This script is automatically executed on first startup via /docker-entrypoint-initdb.d/

echo "Initializing Postgres databases..."

# Get environment variables with defaults
SUPERSET_DB=${SUPERSET_POSTGRES_DB:-superset}
SUPERSET_USER=${SUPERSET_POSTGRES_USER:-superset}
SUPERSET_PASSWORD=${SUPERSET_POSTGRES_PASSWORD}

GRAFANA_DB=${GRAFANA_POSTGRES_DB:-grafana}
GRAFANA_USER=${GRAFANA_POSTGRES_USER:-grafana}
GRAFANA_PASSWORD=${GRAFANA_POSTGRES_PASSWORD}

# Create Superset database and user (if not already created by POSTGRES_DB)
if [ "$POSTGRES_DB" != "$SUPERSET_DB" ]; then
    echo "Creating Superset database: $SUPERSET_DB"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE DATABASE $SUPERSET_DB;
EOSQL
fi

# Create Superset user if it doesn't exist
echo "Creating/updating Superset user: $SUPERSET_USER"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = '$SUPERSET_USER') THEN
            CREATE USER $SUPERSET_USER WITH PASSWORD '$SUPERSET_PASSWORD';
        ELSE
            ALTER USER $SUPERSET_USER WITH PASSWORD '$SUPERSET_PASSWORD';
        END IF;
    END
    \$\$;
    GRANT ALL PRIVILEGES ON DATABASE $SUPERSET_DB TO $SUPERSET_USER;
EOSQL

# Create Grafana database
echo "Creating Grafana database: $GRAFANA_DB"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE $GRAFANA_DB;
EOSQL

# Create Grafana user
echo "Creating/updating Grafana user: $GRAFANA_USER"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = '$GRAFANA_USER') THEN
            CREATE USER $GRAFANA_USER WITH PASSWORD '$GRAFANA_PASSWORD';
        ELSE
            ALTER USER $GRAFANA_USER WITH PASSWORD '$GRAFANA_PASSWORD';
        END IF;
    END
    \$\$;
    GRANT ALL PRIVILEGES ON DATABASE $GRAFANA_DB TO $GRAFANA_USER;
EOSQL

echo "✓ Postgres initialization complete"
echo "  - Superset database: $SUPERSET_DB (user: $SUPERSET_USER)"
echo "  - Grafana database: $GRAFANA_DB (user: $GRAFANA_USER)"

