.PHONY: help setup start dev
.PHONY: infra dbt deps-check

# Main help
help: ## Show this help message
	@echo "Beefy Databarn - Available commands:"
	@echo ""
	@echo "Usage: make <command> [subcommand]"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make infra start       Start infrastructure services"
	@echo "  make infra stop        Stop infrastructure services"
	@echo "  make infra logs        View infrastructure logs"
	@echo "  make infra ps          Show service status"
	@echo ""
	@echo "dbt:"
	@echo "  make dbt run           Run dbt models"
	@echo "  make dbt test          Run dbt tests"
	@echo "  make dbt compile       Compile dbt models"
	@echo "  make dbt docs          Generate and serve documentation"
	@echo ""
	@echo "Dependencies:"
	@echo "  make deps-check        Check for outdated dependencies"
	@echo ""
	@echo "Workflows:"
	@echo "  make setup             Initial setup (copy .env, install deps)"
	@echo "  make start             Start infrastructure and initialize"
	@echo "  make dev               Full development workflow"

# Infrastructure commands - using subcommands
infra:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "start" ]; then \
		$(MAKE) -s _infra-start; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "stop" ]; then \
		$(MAKE) -s _infra-stop; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "logs" ]; then \
		$(MAKE) -s _infra-logs; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "ps" ]; then \
		$(MAKE) -s _infra-ps; \
	else \
		echo "Usage: make infra [start|stop|logs|ps]"; \
		exit 1; \
	fi

# Internal targets
_infra-start:
	@echo "Starting infrastructure services..."
	@set -a && [ -f .env ] && . ./.env && set +a && docker-compose -f infra/dev/docker-compose.yml up -d

_infra-stop:
	@echo "Stopping infrastructure services..."
	@set -a && [ -f .env ] && . ./.env && set +a && docker-compose -f infra/dev/docker-compose.yml down

_infra-logs:
	@set -a && [ -f .env ] && . ./.env && set +a && docker-compose -f infra/dev/docker-compose.yml logs -f

_infra-ps:
	@set -a && [ -f .env ] && . ./.env && set +a && docker-compose -f infra/dev/docker-compose.yml ps

# Catch subcommands (prevents "No rule to make target" errors)
stop logs ps:
	@:

# dbt commands - using subcommands
dbt:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "run" ]; then \
		$(MAKE) -s _dbt-run; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "test" ]; then \
		$(MAKE) -s _dbt-test; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "compile" ]; then \
		$(MAKE) -s _dbt-compile; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "docs" ]; then \
		$(MAKE) -s _dbt-docs; \
	else \
		echo "Usage: make dbt [run|test|compile|docs]"; \
		exit 1; \
	fi

# Internal targets
_dbt-run:
	@echo "Running dbt models..."
	@set -a && [ -f .env ] && . ./.env && set +a && cd dbt && uv run dbt run

_dbt-test:
	@echo "Running dbt tests..."
	@set -a && [ -f .env ] && . ./.env && set +a && cd dbt && uv run dbt test

_dbt-compile:
	@echo "Compiling dbt models..."
	@set -a && [ -f .env ] && . ./.env && set +a && cd dbt && uv run dbt compile

_dbt-docs:
	@echo "Generating dbt documentation..."
	@set -a && [ -f .env ] && . ./.env && set +a && cd dbt && uv run dbt docs generate && dbt docs serve

# Catch subcommands
run test compile docs:
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

# Combined workflow
start: ## Start infrastructure services
	@$(MAKE) -s _infra-start
	@echo ""
	@echo "ℹ️  ClickHouse will initialize automatically on first startup"

dev: ## Full development workflow (setup, start, run dbt)
	@$(MAKE) -s setup
	@$(MAKE) -s _infra-start
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@$(MAKE) -s _dbt-run
	@echo ""
	@echo "✓ Development environment ready!"
	@echo ""
	@echo "Access services:"
	@echo "  - ClickHouse: http://localhost:8123"
	@echo "  - Grafana: http://localhost:3000"
	@echo "  - Prometheus: http://localhost:9090"
	@echo "  - MinIO: http://localhost:9001"

