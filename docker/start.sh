#!/usr/bin/env bash
# Start the local Rust gRPC/MariaDB/MinIO stack. Arguments pass to compose up.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec docker compose -f "$SCRIPT_DIR/docker-compose.yml" up --build "$@"
