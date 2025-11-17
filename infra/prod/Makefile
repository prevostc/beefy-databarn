# Makefile for Docker Compose operations
# This Makefile provides commands for building images and managing the Docker Compose stack
# Uses docker rollout plugin for no-downtime deployments

.PHONY: help build ps logs up deploy ensure-dirs restart

# Load .env file from project root if it exists
ifneq (,$(wildcard ../../.env))
    include ../../.env
    export
endif

# Directories that need to exist for bind mounts
DATA_DIRS := /mnt/data/clickhouse \
             /mnt/data/prometheus \
             /mnt/data/grafana \
             /mnt/data/minio \
             /mnt/data/postgres \
             /mnt/data/superset \
             /mnt/data/redis \
             /mnt/data/traefik_certs \
             /mnt/data/dbt

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

# Prevent make from treating dbt/superset as targets
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
	docker compose -f docker-compose.yml restart $$SERVICE --force-recreate --build; \
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
	docker compose -f docker-compose.yml up -d --build

# down
down:
	docker compose -f docker-compose.yml down


# Build and deploy
deploy: build
	@$(MAKE) -s up

