# beefy-databarn

Indexer for beefy data

# Dev

## Setup

```bash
python3 -m venv .venv
```

```bash
source .venv/bin/activate
```

```bash
pip install . .[test] .[dev]
```

```bash
cp .env.example .env
cp .env-grafana.example .env-grafana
```

```bash
meltano install
```

## Run

### Infra

```bash
./scripts/infra.sh start
```

### start

```bash
# get vault and boost data updated
meltano run tap-beefy-api target-postgres
meltano run tap-github-files target-postgres

# run taps once to initialize the schemas
# this should fail on first run because their input tables do not exist yet
meltano run tap-block-explorer-contract-creation-infos target-postgres
meltano run tap-squid-contract-events target-postgres

# run dbt to create input tables
meltano invoke dbt-postgres:run
meltano invoke dbt-postgres:test

# actually fetch the data
meltano run tap-block-explorer-contract-creation-infos target-postgres
meltano run tap-squid-contract-events target-postgres
```

## Test

### Unit

```
PYTHONPATH=".:src/" pytest
```

### Lint

```bash
ruff check . --fix
```

### Types

```
mypy .
```

### All checks

```
pre-commit run --all-files
```

## Other

### Format SQL files

```bash
# just tell you about the issues
meltano invoke sqlfluff:lint

# fix the issues with your confirmation
meltano invoke sqlfluff:fix

# do not ask for confirmation
meltano invoke sqlfluff:fix-force
```

### Reset venv

```bash
rm -Rf .venv && python3 -m venv .venv && source .venv/bin/activate && pip install . .[test] .[dev]
```

# Links for later

- https://github.com/apache/arrow-datafusion-python/blob/main/examples/sql-parquet.py
- https://github.com/apache/arrow-datafusion/blob/master/datafusion-examples/examples/parquet_sql_multiple_files.rs
- https://wasit7.medium.com/apache-arrow-flight-as-a-data-catalog-3f193e0cad8a
- https://voltrondata.com/resources/running-arrow-flight-server-querying-data-jdbc-adbc
- https://github.com/voltrondata/flight-sql-server-example
- https://dev.to/alexmercedcoder/understanding-rpc-tour-of-api-protocols-grpc-nodejs-walkthrough-and-apache-arrow-flight-55bd
- https://www.influxdata.com/blog/flight-datafusion-arrow-parquet-fdap-architecture-influxdb/
- https://www.influxdata.com/blog/write-database-50-lines-code/
- https://duckdb.org/docs/data/multiple_files/overview
- https://arrow.apache.org/cookbook/py/flight.html
- https://aetperf.github.io/2023/03/30/TPC-H-benchmark-of-Hyper,-DuckDB-and-Datafusion-on-Parquet-files.html
- https://duckdb.org/docs/api/nodejs/overview
