#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docker compose -f "$SCRIPT_DIR/docker-compose.yaml" down --remove-orphans 2>/dev/null || true

# Stop any containers still holding the stack's host ports (e.g. left over from a previous run
# under a different compose project name or container_name).
for port in 3307 9091 8081 50051; do
    ids=$(docker ps -q --filter "publish=$port")
    [ -n "$ids" ] && docker stop $ids 2>/dev/null || true
done

docker compose -f "$SCRIPT_DIR/docker-compose.yaml" up --build "$@"
