# Makefile for Docker Compose operations
# This Makefile provides commands for building images and managing the Docker Compose stack
# Uses docker rollout plugin for no-downtime deployments

.PHONY: help build ps logs up deploy

# Load .env file from project root if it exists
ifneq (,$(wildcard ../../.env))
    include ../../.env
    export
endif



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

# up (and build)
up:
	docker compose -f docker-compose.yml up -d --build

# down
down:
	docker compose -f docker-compose.yml down


# Build and deploy
deploy: build
	@$(MAKE) -s up

