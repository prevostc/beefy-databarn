#!/bin/bash
set -e

echo_step() {
cat <<EOF
######################################################################
Init Step ${1}/${STEP_CNT} [${2}] -- ${3}
######################################################################
EOF
}
# Wait for Superset to be ready
echo "Waiting for Superset to be ready..."
sleep 5

# upgrade database
echo_step "1" "Starting" "Applying DB migrations"
superset db upgrade

# Create admin user if it doesn't exist

SUPERSET_ADMIN_PASSWORD="${SUPERSET_ADMIN_PASSWORD:-admin}"
echo_step "2" "Starting" "Setting up admin user ( admin / $SUPERSET_ADMIN_PASSWORD )"
superset fab create-admin \
    --username ${SUPERSET_ADMIN_USER:-admin} \
    --email ${SUPERSET_ADMIN_EMAIL:-admin@example.com} \
    --password ${SUPERSET_ADMIN_PASSWORD:-changeme} \
    --firstname ${SUPERSET_ADMIN_FIRSTNAME:-Admin} \
    --lastname ${SUPERSET_ADMIN_LASTNAME:-User}

# ClickHouse is already healthy when this script runs (via depends_on healthcheck)

echo_step "3" "Starting" "Setting up roles and perms"
superset init

# Run Python script to add ClickHouse connection
echo_step "4" "Starting" "Adding ClickHouse database connection"
python3 /usr/local/bin/provision/add-clickhouse-connection.py

echo_step "5" "Completed" "Superset initialization complete"

