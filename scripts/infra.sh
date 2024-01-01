#!/bin/sh

# run docker container

usage() {
    echo "Usage: $0 [start|stop|logs]"
}

if [ $# -ne 1 ]; then
    usage
    exit 1
fi

case $1 in
    start)
        docker compose -p databarn-dev -f infra/docker-compose.dev.yml up -d
        ;;
    stop)
        docker compose -p databarn-dev -f infra/docker-compose.dev.yml logs -f
        ;;
    logs)
        docker compose -p databarn-dev -f infra/docker-compose.dev.yml down
        ;;
    *)
        usage
        exit 1
esac
