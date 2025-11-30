FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (for better layer caching)
COPY dbt/pyproject.toml dbt/uv.lock /app/dbt/

# Install Python dependencies using uv from dbt project
WORKDIR /app/dbt
RUN uv sync --frozen

# Copy rest of dbt project files
COPY dbt/ /app/dbt/

# Set working directory to dbt project
WORKDIR /app/dbt

# Install dbt packages
RUN uv run dbt deps

# Create logs directory
RUN mkdir -p /app/dbt/logs

# Copy infra scripts (scheduler is deployment-specific)
COPY infra/dbt/scheduler.py /app/infra/dbt/scheduler.py
RUN chmod +x /app/infra/dbt/scheduler.py

# Set environment variables
ENV DBT_PROFILES_DIR=/app/dbt
ENV DBT_PROJECT_DIR=/app/dbt

# Run the Python scheduler (runs every 30 minutes)
CMD ["uv", "run", "/app/infra/dbt/scheduler.py"]

