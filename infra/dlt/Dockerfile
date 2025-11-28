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
COPY pyproject.toml uv.lock /app/

# Install Python dependencies using uv
WORKDIR /app
RUN uv sync --frozen

# Copy dlt project files
COPY dlt/ /app/dlt/

# Copy infra scripts (scheduler is deployment-specific)
COPY infra/dlt/scheduler.py /app/infra/dlt/scheduler.py
COPY infra/dlt/run.py /app/infra/dlt/run.py
RUN chmod +x /app/infra/dlt/scheduler.py /app/infra/dlt/run.py

# Set working directory to dlt project
WORKDIR /app/dlt

# Run the Python scheduler (runs every 5 minutes)
CMD ["uv", "run", "/app/infra/dlt/scheduler.py"]

