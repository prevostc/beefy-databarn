FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    cron \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (for better layer caching)
COPY pyproject.toml uv.lock /app/

# Install Python dependencies using uv
WORKDIR /app
RUN uv sync --frozen

# Copy dlt project files
COPY dlt/ /app/dlt/

# Set working directory to dlt project
WORKDIR /app/dlt

# Copy infra scripts and crontab (these change more often)
COPY infra/dlt/run_dlt.sh /app/infra/dlt/run_dlt.sh
COPY infra/dlt/crontab /app/infra/dlt/crontab
COPY infra/dlt/docker-entrypoint.sh /app/infra/dlt/docker-entrypoint.sh
RUN chmod +x /app/infra/dlt/run_dlt.sh /app/infra/dlt/docker-entrypoint.sh

# Install crontab
RUN crontab /app/infra/dlt/crontab

# Use entrypoint script to export environment variables for cron
ENTRYPOINT ["/app/infra/dlt/docker-entrypoint.sh"]

