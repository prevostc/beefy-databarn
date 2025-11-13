# Beefy Databarn

Full stack data platform for federating multiple external data sources with ClickHouse, dbt, monitoring, and API capabilities.

## Architecture

- **ClickHouse**: Target database with federation capabilities
- **dbt**: Data modeling with staging → intermediate → marts layers
- **Traefik**: Routing, HTTPS termination, blue-green deployment
- **Prometheus + Grafana**: Metrics collection, monitoring, and alerting
- **MinIO**: S3-compatible blob storage
- **Docker Swarm**: Deployment and orchestration

## Quick Start

### Prerequisites

- Docker and Docker Compose (for local development)
- Docker Swarm (for production deployment)
- uv (Python package manager)
- Access to external PostgreSQL "beefy-db" database

### Local Development Setup

**Quick start (automated):**
```bash
make setup          # Setup .env and install dependencies
make start          # Start infrastructure and initialize
make dbt run        # Run dbt models
```

**Manual setup:**

1. Clone the repository and run initial setup:
   ```bash
   make setup
   # Or manually:
   cp .env.example .env  # Edit .env with your credentials
   uv sync               # Install dependencies
   ```

2. Start infrastructure services:
   ```bash
   make infra start      # Start ClickHouse, Grafana, Prometheus, MinIO
   ```

3. Initialize ClickHouse:
   ```bash
   make infra init       # Create analytics database
   ```

4. Run dbt models:
   ```bash
   make dbt run          # Run all models
   make dbt test         # Run tests
   ```

5. Access services:
   - ClickHouse: http://localhost:8123
   - Grafana: http://localhost:3000 (admin/changeme by default)
   - Prometheus: http://localhost:9090
   - MinIO Console: http://localhost:9001 (admin/changeme by default)
   - MinIO API: http://localhost:9002

**Other useful commands:**
```bash
make help             # Show all available commands
make infra logs       # View infrastructure logs
make infra ps         # Check service status
make infra stop       # Stop all services
```

### Production Deployment (Docker Swarm)

1. Follow steps 1-3 from Local Development
2. Deploy infrastructure:
   ```bash
   # For development environment
   ./scripts/deploy.sh dev

   # For production environment
   ./scripts/deploy.sh prod
   ```

3. Initialize ClickHouse:
   ```bash
   ./infra/clickhouse/init-clickhouse.sh
   ```

4. Run dbt models:
   ```bash
   uv run dbt-run
   ```

## Project Structure

- `infra/`: Infrastructure configurations (dev/prod stacks, ClickHouse, Traefik, monitoring)
- `dbt/`: dbt project with staging, intermediate, and marts models
- `scripts/`: Deployment and setup scripts
- `dlt/`: Future data ingestion pipelines
- `hasura/`: Future GraphQL API setup

## dbt Models

The dbt project follows best practices with three layers:

1. **Staging** (`models/staging/`): ClickHouse external tables pointing to source databases
2. **Intermediate** (`models/intermediate/`): Cleanup and transformations
3. **Marts** (`models/marts/`): Business-ready models for consumption

## Development

### Available Commands

View all commands:
```bash
make help
```

### Infrastructure Management
```bash
make infra start    # Start infrastructure services
make infra stop     # Stop infrastructure services
make infra logs     # View logs
make infra ps       # Check service status
make infra init     # Initialize ClickHouse
```

### dbt Commands
```bash
make dbt run        # Run all models
make dbt test       # Run tests
make dbt compile    # Compile models
make dbt docs       # Generate and serve documentation
```

### Full Workflows
```bash
make dev            # Complete setup: install deps, start services, run dbt
make start          # Start infrastructure and initialize
```

### Direct Commands (without make)

If you prefer to use docker-compose and dbt directly:
```bash
# Infrastructure
docker-compose up -d
docker-compose logs -f
docker-compose down

# dbt
cd dbt && uv run dbt run
cd dbt && uv run dbt test
```

## Deployment

The platform supports both development and production environments:

- Development: `infra/dev/docker-stack.yml`
- Production: `infra/prod/docker-stack.yml`

Services can be moved between nodes using Docker Swarm labels.

## Monitoring

- Grafana: http://localhost:3000 (default)
- Prometheus: http://localhost:9090
- ClickHouse: http://localhost:8123

## Security

- UFW firewall configuration via `scripts/setup.sh`
- fail2ban rate limiting via `scripts/setup.sh`
- Traefik HTTPS termination with Let's Encrypt

