# Makefile for Docker Compose operations
# This Makefile provides commands for building images and managing the Docker Compose stack
# Uses docker rollout plugin for no-downtime deployments

.PHONY: help build ps logs up deploy ensure-dirs restart dbt dlt clickhouse

# Load .env file from project root if it exists
ifneq (,$(wildcard ../../.env))
    include ../../.env
    export
endif

# Directories that need to exist for bind mounts
# Use ?= to allow .env file to override, otherwise default to /mnt/data
STORAGE_DIR ?= /mnt/data
DATA_DIRS := $(STORAGE_DIR)/clickhouse \
             $(STORAGE_DIR)/prometheus \
             $(STORAGE_DIR)/grafana \
             $(STORAGE_DIR)/minio \
             $(STORAGE_DIR)/postgres \
             $(STORAGE_DIR)/superset \
             $(STORAGE_DIR)/redis \
             $(STORAGE_DIR)/traefik_certs \
             $(STORAGE_DIR)/dbt

# Default target
.DEFAULT_GOAL := help

# Show help message
help:
	@echo "Available commands:"
	@echo ""
	@echo "  make help              Show this help message (default)"
	@echo "  make build [dbt|superset]  Build images (all or specific service)"
	@echo "  make ps                Show running services"
	@echo "  make logs              Show logs from all services"
	@echo "  make up                Rolling update (deploy with no-downtime)"
	@echo "  make deploy            Build and deploy"
	@echo "  make restart <service> Restart a specific service (service name required)"
	@echo "  make dbt run           Run dbt in the dbt container"
	@echo "  make dbt test          Test dbt in the dbt container"
	@echo "  make dlt run            Run dlt in the dlt container"
	@echo "  make clickhouse client Open ClickHouse client"
	@echo "  make ensure-dirs       Create required data directories"
	@echo ""

# Build images: make build [dbt|superset] or make build (all)
# Usage: make build or make build dbt or make build superset
build:
	@if [ -n "$(filter dbt superset,$(MAKECMDGOALS))" ]; then \
		SERVICE=$(filter dbt superset,$(MAKECMDGOALS)); \
		echo "Building $$SERVICE image..."; \
		docker compose -f docker-compose.yml build $$SERVICE; \
		echo "$$SERVICE image built successfully"; \
	else \
		echo "Building all images..."; \
		docker compose -f docker-compose.yml build; \
		echo "All images built successfully"; \
	fi

# Run dbt: make dbt run or make dbt test
# Usage: make dbt run or make dbt test
dbt:
	@if [ -n "$(filter run,$(MAKECMDGOALS))" ]; then \
		echo "Running dbt..."; \
		docker exec -it beefy-databarn-dbt-1 uv run dbt run; \
	elif [ -n "$(filter test,$(MAKECMDGOALS))" ]; then \
		echo "Testing dbt..."; \
		docker exec -it beefy-databarn-dbt-1 uv run dbt test; \
	else \
		echo "Usage: make dbt run or make dbt test"; \
		exit 1; \
	fi

# Run dlt: make dlt run
# Usage: make dlt run
dlt:
	@if [ -n "$(filter run,$(MAKECMDGOALS))" ]; then \
		echo "Running dlt..."; \
		docker exec -it beefy-databarn-dlt-1 uv run /app/infra/dlt/run.py; \
	else \
		echo "Usage: make dlt run"; \
		exit 1; \
	fi

# ClickHouse client: make clickhouse client
# Usage: make clickhouse client
clickhouse:
	@if [ -n "$(filter client,$(MAKECMDGOALS))" ]; then \
		echo "Opening ClickHouse client..."; \
		docker exec -it beefy-databarn-clickhouse-1 clickhouse-client; \
	else \
		echo "Usage: make clickhouse client"; \
		exit 1; \
	fi

# Prevent make from treating dbt/superset/run/test/client as targets
%:
	@:

# Show running services
ps:
	docker compose -f docker-compose.yml ps

# Show logs from all services
logs:
	docker compose -f docker-compose.yml logs -f

# Restart services: make restart <service_name>
# Usage: make restart <service_name>
restart:
	@SERVICE=$(filter-out restart,$(MAKECMDGOALS)); \
	if [ -z "$$SERVICE" ]; then \
		echo "Error: Service name is required"; \
		echo "Usage: make restart <service_name>"; \
		exit 1; \
	fi; \
	echo "Restarting $$SERVICE..."; \
	docker compose -f docker-compose.yml up -d $$SERVICE --force-recreate --build; \
	echo "$$SERVICE restarted successfully"

# Ensure required directories exist for bind mounts
ensure-dirs:
	@echo "Creating required data directories..."
	@for dir in $(DATA_DIRS); do \
		if [ ! -d "$$dir" ]; then \
			echo "Creating $$dir..."; \
			sudo mkdir -p "$$dir"; \
			sudo chmod 755 "$$dir"; \
		else \
			echo "Directory $$dir already exists"; \
		fi; \
	done
	@echo "✓ All required directories created"

# up (and build)
up: ensure-dirs
	docker compose -f docker-compose.yml up -d

# down
down:
	docker compose -f docker-compose.yml down


# Build and deploy
deploy: build
	@$(MAKE) -s up

