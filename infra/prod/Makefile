# Makefile for Docker Compose operations
# This Makefile provides commands for building images and managing the Docker Compose stack
# Uses docker rollout plugin for no-downtime deployments

.PHONY: help build ps logs deploy ensure-dirs restart dbt dlt clickhouse run test client github_files tvls beefy_db beefy_api

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


infra: ensure-dirs
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		start) \
			echo "Starting infrastructure services (rebuilding images if needed)..."; \
			$(DC) up -d --build; \
			echo "✓ Infrastructure services started"; \
			$(MAKE) -s _print-urls \
			;; \
		stop) \
			echo "Stopping infrastructure services..."; \
			$(DC) down; \
			echo "✓ Infrastructure services stopped"; \
			;; \
		restart) \
			echo "Restarting infrastructure services..."; \
			$(DC) restart; \
			echo "✓ Infrastructure services restarted"; \
			;; \
		logs) \
			$(DC) logs -f; \
			;; \
		ps) \
			$(DC) ps; \
			;; \
		help|"") \
			echo "infra:"; \
			echo "  make infra start         Start infrastructure services (rebuilds if needed)"; \
			echo "  make infra stop          Stop infrastructure services"; \
			echo "  make infra restart       Restart infrastructure services"; \
			echo "  make infra logs          View infrastructure logs"; \
			echo "  make infra ps            Show service status"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make infra [start|stop|restart|logs|ps|help]"; \
			exit 1; \
		;; \
	esac

# Run dbt: make dbt run or make dbt test
# Usage: make dbt run or make dbt test
dbt: ensure-dirs
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		logs) \
			echo "$$SUBCMD dbt..."; \
			$(DC) exec dbt logs -f; \
		;; \
		restart) \
			echo "Restarting dbt..."; \
			$(DC) restart dbt --force-recreate --build; \
		;; \
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
		logs) \
			echo "Showing dlt logs..."; \
			$(DC) exec dlt logs -f; \
		;; \
		restart) \
			echo "Restarting dlt..."; \
			$(DC) restart dlt --force-recreate --build; \
		;; \
		ps) \
			echo "Showing dlt service status..."; \
			$(DC) ps dlt; \
		;; \
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
		logs) \
			echo "Showing ClickHouse logs..."; \
			$(DC) exec clickhouse logs -f; \
		;; \
		restart) \
			echo "Restarting ClickHouse..."; \
			$(DC) restart clickhouse --force-recreate --build; \
		;; \
		ps) \
			echo "Showing ClickHouse service status..."; \
			$(DC) ps clickhouse; \
		;; \
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

