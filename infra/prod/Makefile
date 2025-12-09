# Makefile for Docker Compose operations
# This Makefile provides commands for building images and managing the Docker Compose stack
# Uses docker rollout plugin for no-downtime deployments

.PHONY: help build ps logs up deploy ensure-dirs restart dbt dlt clickhouse run test client github_files tvls beefy_db beefy_api

# Paths and common commands
ROOT_DIR := $(abspath ../..)
DC := docker compose -f $(ROOT_DIR)/infra/prod/docker-compose.yml --env-file $(ROOT_DIR)/.env
UV := uv run --env-file $(ROOT_DIR)/.env

# Directories that need to exist for bind mounts (override via .env if present)
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
	@$(MAKE) -s build help
	@$(MAKE) -s dbt help
	@$(MAKE) -s dlt help
	@$(MAKE) -s clickhouse help
	@$(MAKE) -s infra help
	@$(MAKE) -s misc help

# Build images: make build [dbt|superset] or make build (all)
# Usage: make build or make build dbt or make build superset
build:
	@if [ -n "$(filter dbt superset,$(MAKECMDGOALS))" ]; then \
		SERVICE=$(filter dbt superset,$(MAKECMDGOALS)); \
		echo "Building $$SERVICE image..."; \
		$(DC) build $$SERVICE; \
		echo "$$SERVICE image built successfully"; \
	else \
		echo "Building all images..."; \
		$(DC) build; \
		echo "All images built successfully"; \
	fi

# Show running services
ps:
	$(DC) ps

# Show logs from all services
logs:
	@SERVICE=$(filter-out logs,$(MAKECMDGOALS)); \
	if [ -n "$$SERVICE" ]; then \
		echo "Showing logs for $$SERVICE..."; \
		$(DC) logs -f $$SERVICE; \
	else \
		echo "Showing logs for all services..."; \
		$(DC) logs -f; \
	fi;

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
	$(DC) up -d $$SERVICE --force-recreate --build; \
	echo "$$SERVICE restarted successfully"

# up (and build)
up: ensure-dirs
	$(DC) up -d

# down
down:
	$(DC) down

# Run dbt: make dbt run or make dbt test
# Usage: make dbt run or make dbt test
dbt:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		run) \
			echo "Running dbt..."; \
			MODEL="$(word 3,$(MAKECMDGOALS))" && \
			if [ -n "$$MODEL" ]; then \
				echo "Running dbt model: $$MODEL..."; \
				$(DC) exec dbt uv run dbt run --select $$MODEL; \
			else \
				echo "Running all dbt models..."; \
				$(DC) exec dbt uv run dbt run; \
			fi; \
			echo "dbt run completed successfully"; \
		;; \
		test) \
			echo "Testing dbt..."; \
			MODEL="$(word 3,$(MAKECMDGOALS))" && \
			if [ -n "$$MODEL" ]; then \
				echo "Testing dbt model: $$MODEL..."; \
				$(DC) exec dbt uv run dbt test --select $$MODEL; \
			else \
				echo "Testing all dbt models..."; \
				$(DC) exec dbt uv run dbt test; \
			fi; \
			echo "dbt test completed successfully"; \
		;; \
		help|"") \
			echo "dbt:"; \
			echo "  make dbt run [<model>]   Run dbt models"; \
			echo "  make dbt test [<model>]   Run dbt tests"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make dbt [run [<model>]|test [<model>]|help]"; \
			exit 1; \
		;; \
	esac

# Run dlt: make dlt run
# Usage: make dlt run
dlt:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		run) \
			echo "Running dlt..."; \
			SOURCE="$(word 3,$(MAKECMDGOALS))" && \
			RESOURCE="$(word 4,$(MAKECMDGOALS))" && \
			if [ -n "$$RESOURCE" ] && [ -n "$$SOURCE" ]; then \
				echo "Running dlt source: $$SOURCE, resource: $$RESOURCE..."; \
				$(DC) exec dlt uv run /app/dlt/$${SOURCE}_pipeline.py $$RESOURCE; \
			elif [ -n "$$SOURCE" ]; then \
				echo "Running dlt source: $$SOURCE..."; \
				$(DC) exec dlt uv run /app/dlt/$${SOURCE}_pipeline.py; \
			else \
				echo "Usage: make dlt run <source> [resource]"; \
				exit 1; \
			fi; \
		;; \
		help|"") \
			echo "dlt:"; \
			echo "  make dlt run [<pipeline>]   Run dlt pipeline"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make dlt [run [<pipeline>]|help]"; \
			exit 1; \
		;; \
	esac

# ClickHouse client: make clickhouse client
# Usage: make clickhouse client
ch: clickhouse # alias for clickhouse
clickhouse:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		client) \
			echo "Opening ClickHouse client..."; \
			$(DC) exec clickhouse clickhouse-client; \
		;; \
		help|"") \
			echo "clickhouse:"; \
			echo "  make clickhouse client   Open ClickHouse client"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make clickhouse [client|help]"; \
			exit 1; \
		;; \
	esac

# Prevent make from treating command arguments as targets
run test client github_files tvls beefy_db beefy_api:
	@:

# Catch-all for any other resource/source names
%:
	@:

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


# Build and deploy
deploy: build
	@$(MAKE) -s up

