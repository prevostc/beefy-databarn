# Hasura GraphQL API

This directory will contain Hasura configuration for exposing data via GraphQL API.

## Future Setup

When ready to add Hasura:

1. Add Hasura service to Docker Swarm stack
2. Configure metadata and relationships
3. Set up authentication and authorization
4. Connect to ClickHouse via Hasura's ClickHouse connector (when available) or via REST endpoints

## Example Structure

```
hasura/
├── config.yaml
├── metadata/
│   ├── databases/
│   └── actions/
└── migrations/
```

