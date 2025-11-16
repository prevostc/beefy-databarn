# Makefile for Docker Swarm operations
# This Makefile provides commands for building images and managing the Docker Swarm stack

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
	@echo "  make up                Rolling update (deploy stack)"
	@echo "  make deploy            Build and deploy"
	@echo ""

# Build images: make build [dbt|superset] or make build (all)
# Usage: make build or make build dbt or make build superset
build:
	@if [ -n "$(filter dbt superset,$(MAKECMDGOALS))" ]; then \
		SERVICE=$(filter dbt superset,$(MAKECMDGOALS)); \
		echo "Building $$SERVICE image..."; \
		docker compose -f docker docker-compose.build.yml build $$SERVICE; \
		echo "$$SERVICE image built successfully"; \
	else \
		echo "Building all images..."; \
		docker compose -f docker docker-compose.build.yml build; \
		echo "All images built successfully"; \
	fi

# Prevent make from treating dbt/superset as targets
%:
	@:

# Show running services
ps:
	docker stack services beefy-databarn

# Show logs from all services
logs:
	docker service logs -f beefy-databarn

# Rolling update (deploy stack)
up:
	docker stack deploy -c docker-stack.yml beefy-databarn

# Build and deploy
deploy: build
	@$(MAKE) -s up

