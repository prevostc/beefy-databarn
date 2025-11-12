# dlt Pipelines

This directory will contain data ingestion pipelines using dlt (data load tool).

## Future Setup

When ready to add dlt pipelines:

1. Create pipeline files (e.g., `beefy_db_pipeline.py`)
2. Configure sources and destinations
3. Add to Docker Swarm stack for scheduled execution
4. Store artifacts in MinIO

## Example Structure

```
dlt/
├── pipelines/
│   └── beefy_db_pipeline.py
├── sources/
│   └── beefy_db.py
└── requirements.txt
```

