.PHONY: help setup start dev
.PHONY: infra dbt dlt grafana clickhouse deps-check

# Load .env file if it exists
ifneq (,$(wildcard ./.env))
    include ./.env
    export
endif

# Prevent execution in production (user "databarn")
CURRENT_USER := $(shell whoami 2>/dev/null)
ifeq ($(CURRENT_USER),databarn)
    $(error This Makefile should not be run in production. Use infra/prod/Makefile instead.)
endif

# Main help
help: ## Show this help message
	@echo "Beefy Databarn - Available commands:"
	@echo ""
	@echo "Usage: make <command> [subcommand]"
	@echo ""
	@echo "dlt:"
	@echo "  make dlt run             Run the Beefy API dlt pipeline"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make infra start         Start infrastructure services (rebuilds if needed)"
	@echo "  make infra build         Rebuild infrastructure images"
	@echo "  make infra stop          Stop infrastructure services"
	@echo "  make infra restart       Restart infrastructure services"
	@echo "  make infra logs          View infrastructure logs"
	@echo "  make infra ps            Show service status"
	@echo ""
	@echo "dbt:"
	@echo "  make dbt run             Run dbt models"
	@echo "  make dbt run <model>     Run a specific dbt model"
	@echo "  make dbt test            Run dbt tests"
	@echo "  make dbt compile         Compile dbt models"
	@echo "  make dbt sql [<model>]   Show compiled SQL (optionally for specific model)"
	@echo "  make dbt docs            Generate and serve documentation"
	@echo ""
	@echo "Grafana:"
	@echo "  make grafana restart     Re-restart Grafana (reload configs)"
	@echo ""
	@echo "ClickHouse:"
	@echo "  make clickhouse restart  Re-restart ClickHouse (reload configs)"
	@echo "  make clickhouse client   Open ClickHouse client shell"
	@echo ""
	@echo "Dependencies:"
	@echo "  make deps-check          Check for outdated dependencies"
	@echo ""
	@echo "Workflows:"
	@echo "  make setup               Initial setup (copy .env, install deps)"
	@echo "  make start               Start infrastructure and initialize"
	@echo "  make dev                 Full development workflow"

# Infrastructure commands - using subcommands
infra:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "start" ]; then \
		$(MAKE) -s _infra-start; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "build" ]; then \
		$(MAKE) -s _infra-build; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "stop" ]; then \
		$(MAKE) -s _infra-stop; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "restart" ]; then \
		$(MAKE) -s _infra-restart; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "logs" ]; then \
		$(MAKE) -s _infra-logs; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "ps" ]; then \
		$(MAKE) -s _infra-ps; \
	else \
		echo "Usage: make infra [start|build|stop|restart|logs|ps]"; \
		exit 1; \
	fi

# Internal targets
_infra-start:
	@echo "Starting infrastructure services (rebuilding images if needed)..."
	@docker compose -f infra/dev/docker-compose.yml up -d --build
	@echo "✓ Infrastructure services started"
	@$(MAKE) -s _print-urls

_infra-build:
	@echo "Rebuilding infrastructure images..."
	@docker compose -f infra/dev/docker-compose.yml build
	@echo "✓ Infrastructure images rebuilt"

_infra-stop:
	@echo "Stopping infrastructure services..."
	@docker compose -f infra/dev/docker-compose.yml down

_infra-logs:
	@docker compose -f infra/dev/docker-compose.yml logs -f

_infra-ps:
	@docker compose -f infra/dev/docker-compose.yml ps

_infra-restart:
	@echo "Restarting infrastructure services..."
	@docker compose -f infra/dev/docker-compose.yml restart
	@echo "✓ Infrastructure services restarted"

# Catch subcommands (prevents "No rule to make target" errors)
start build stop logs ps:
	@:

# dbt commands - using subcommands
dbt:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "run" ]; then \
		$(MAKE) -s _dbt-run MODEL=""; \
	elif [ "$(word 2,$(MAKECMDGOALS))" = "run" ]; then \
		$(MAKE) -s _dbt-run MODEL="$(word 3,$(MAKECMDGOALS))"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "test" ]; then \
		$(MAKE) -s _dbt-test MODEL=""; \
	elif [ "$(word 2,$(MAKECMDGOALS))" = "test" ]; then \
		$(MAKE) -s _dbt-test MODEL="$(word 3,$(MAKECMDGOALS))"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "compile" ]; then \
		$(MAKE) -s _dbt-compile; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "sql" ]; then \
		$(MAKE) -s _dbt-sql MODEL=""; \
	elif [ "$(word 2,$(MAKECMDGOALS))" = "sql" ]; then \
		$(MAKE) -s _dbt-sql MODEL="$(word 3,$(MAKECMDGOALS))"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "docs" ]; then \
		$(MAKE) -s _dbt-docs; \
	else \
		echo "Usage: make dbt [run [model]|test|compile|sql [model_name]|docs]"; \
		exit 1; \
	fi

# Internal targets
_dbt-run:
	@if [ -n "$(MODEL)" ]; then \
		echo "Running dbt model: $(MODEL)..."; \
		cd dbt && uv run --env-file ../.env dbt run --select $(MODEL) --show-all-deprecations; \
	else \
		echo "Running dbt models..."; \
		cd dbt && uv run --env-file ../.env dbt run --show-all-deprecations; \
	fi

_dbt-test:
	@if [ -n "$(MODEL)" ]; then \
		echo "Running dbt tests for model: $(MODEL)..."; \
		cd dbt && uv run --env-file ../.env dbt test --select $(MODEL); \
	else \
		echo "Running dbt tests..."; \
		cd dbt && uv run --env-file ../.env dbt test; \
	fi

_dbt-compile:
	@echo "Compiling dbt models..."
	@cd dbt && uv run --env-file ../.env dbt compile

_dbt-sql:
	@echo "Compiling and showing SQL (no queries executed)..."
	@cd dbt && \
	if [ -n "$(MODEL)" ]; then \
		uv run --env-file ../.env dbt compile --select $(MODEL) > /dev/null 2>&1 && \
		COMPILED_FILE=$$(find target/compiled/beefy_databarn/models -name "$(MODEL).sql" -type f | head -1) && \
		if [ -n "$$COMPILED_FILE" ]; then \
			echo "=== Compiled SQL for $(MODEL) ===" && \
			cat "$$COMPILED_FILE"; \
		else \
			echo "Error: Could not find compiled SQL for $(MODEL)"; \
			exit 1; \
		fi; \
	else \
		uv run --env-file ../.env dbt compile > /dev/null 2>&1 && \
		echo "Compiled SQL files are in target/compiled/beefy_databarn/models/"; \
		find target/compiled/beefy_databarn/models -name "*.sql" -type f | head -10; \
	fi

_dbt-docs:
	@echo "Generating dbt documentation..."
	@cd dbt && uv run --env-file ../.env dbt docs generate && uv run --env-file ../.env dbt docs serve

# Catch subcommands
run test compile sql docs:
	@:

# dlt commands - using subcommands
dlt:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "run" ]; then \
		$(MAKE) -s _dlt-run; \
	else \
		echo "Usage: make dlt [run]"; \
		exit 1; \
	fi

_dlt-run:
	@echo "Running Beefy API dlt pipeline..."
	@cd dlt && uv run --env-file ../.env ./run.py

# Catch unknown commands - show help and exit with error
# Only show error if this is the first goal (not a model name or subcommand argument)
%:
	@if [ "$@" = "$(firstword $(MAKECMDGOALS))" ]; then \
		echo "Error: Unknown command '$@'" >&2; \
		echo "" >&2; \
		$(MAKE) -s help >&2; \
		exit 1; \
	fi

# Grafana commands - using subcommands
grafana:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "restart" ]; then \
		$(MAKE) -s _grafana-restart; \
	else \
		echo "Usage: make grafana [restart]"; \
		exit 1; \
	fi

# Internal targets
_grafana-restart:
	@echo "Re-restarting Grafana (restarting service to reload configs)..."
	@docker compose -f infra/dev/docker-compose.yml restart grafana
	@echo "✓ Grafana re-restarted"

clickhouse:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "restart" ]; then \
		$(MAKE) -s _clickhouse-restart; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "client" ]; then \
		$(MAKE) -s _clickhouse-client; \
	else \
		echo "Usage: make clickhouse [restart|client]"; \
		exit 1; \
	fi

_clickhouse-restart:
	@echo "Restarting ClickHouse..."
	@docker compose -f infra/dev/docker-compose.yml restart clickhouse
	@echo "✓ ClickHouse restarted"

_clickhouse-client:
	@echo "Opening ClickHouse client shell..."
	@docker compose -f infra/dev/docker-compose.yml exec clickhouse clickhouse-client

# Catch subcommands
restart client:
	@:

# Dependencies
deps-check: ## Check for outdated dependencies
	@echo "Checking for outdated dependencies..."
	@echo ""
	@echo "Currently installed packages:"
	@uv pip list
	@echo ""
	@echo "Checking for available updates..."
	@uv pip list --outdated 2>/dev/null || (echo "No outdated packages found (or command not available)" && echo "")
	@echo ""
	@echo "To update dependencies:"
	@echo "  1. Edit pyproject.toml with desired version constraints"
	@echo "  2. Run: uv lock --upgrade"
	@echo "  3. Run: uv sync"

# Setup commands
setup: ## Initial setup (copy .env, install deps)
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✓ Created .env file - please edit it with your credentials"; \
	else \
		echo "✓ .env file already exists"; \
	fi
	uv sync
	@echo "✓ Dependencies installed"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Edit .env with your credentials"
	@echo "  2. make infra start"
	@echo "  3. make dbt run"

dev: ## Full development workflow (setup, start, run dbt)
	@$(MAKE) -s setup
	@$(MAKE) -s _infra-start
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@$(MAKE) -s _dbt-run
	@echo ""
	@echo "✓ Development environment ready!"
	@$(MAKE) -s _print-urls

# Shared utility targets
_print-urls:
	@echo ""
	@echo "Access services:"
	@echo "  - Superset: http://localhost:8088" && \
	echo "  - Traefik Dashboard: http://localhost:8080" && \
	echo "  - ClickHouse: http://localhost:8123 ($${CLICKHOUSE_USER:-default}/$${CLICKHOUSE_PASSWORD:-<set in .env>})" && \
	echo "  - Grafana: http://localhost:3000 ($${GRAFANA_ADMIN_USER:-admin}/$${GRAFANA_ADMIN_PASSWORD:-admin})" && \
	echo "  - Prometheus: http://localhost:9090 (no auth)" && \
	echo "  - MinIO: http://localhost:9001 ($${MINIO_ACCESS_KEY:-admin}/$${MINIO_SECRET_KEY:-admin})"

