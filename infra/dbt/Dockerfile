FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (for better layer caching)
COPY pyproject.toml uv.lock /app/

# Install Python dependencies using uv
WORKDIR /app
RUN uv sync --frozen

# Copy dbt project files
COPY dbt/ /app/dbt/

# Set working directory to dbt project
WORKDIR /app/dbt

# Install dbt packages (this changes less often, so do it before crontab)
RUN uv run dbt deps

# Create logs directory
RUN mkdir -p /app/dbt/logs

# Copy infra scripts and crontab (these change more often)
COPY infra/dbt/run_dbt.sh /app/infra/dbt/run_dbt.sh
COPY infra/dbt/crontab /app/infra/dbt/crontab
RUN chmod +x /app/infra/dbt/run_dbt.sh

# Install crontab
RUN crontab /app/infra/dbt/crontab

# Set environment variables
ENV DBT_PROFILES_DIR=/app/dbt
ENV DBT_PROJECT_DIR=/app/dbt

# Default command: run cron in foreground (can be overridden in docker-stack.yml)
CMD ["cron", "-f"]

