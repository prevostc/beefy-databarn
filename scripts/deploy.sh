#!/bin/bash
set -e

# Docker Swarm deployment script
# Usage: ./deploy.sh [dev|prod]

ENVIRONMENT="${1:-dev}"

if [[ ! "$ENVIRONMENT" =~ ^(dev|prod)$ ]]; then
  echo "Error: Environment must be 'dev' or 'prod'"
  exit 1
fi

echo "Deploying $ENVIRONMENT environment..."

# Check if .env file exists
if [ ! -f .env ]; then
  echo "Error: .env file not found. Please copy .env.example to .env and configure it."
  exit 1
fi

# Load environment variables
export $(cat .env | grep -v '^#' | xargs)

# Check if Docker Swarm is initialized
if ! docker info | grep -q "Swarm: active"; then
  echo "Initializing Docker Swarm..."
  docker swarm init
fi

# Deploy stack
STACK_NAME="databarn-${ENVIRONMENT}"
STACK_FILE="infra/${ENVIRONMENT}/docker-stack.yml"

echo "Deploying stack: $STACK_NAME"
docker stack deploy -c "$STACK_FILE" "$STACK_NAME" --with-registry-auth

echo "Waiting for services to start..."
sleep 10

# Show service status
echo ""
echo "Stack services:"
docker stack services "$STACK_NAME"

echo ""
echo "Service status:"
docker stack ps "$STACK_NAME"

echo ""
echo "✓ Deployment complete!"
echo ""
echo "To view logs: docker service logs -f <service_name>"
echo "To scale a service: docker service scale <service_name>=<replicas>"

