#!/usr/bin/env bash
#
# Start the CLP Service stack (infrastructure + coordinator nodes).
#
# Ensures the CLP core .deb package is present before starting, then
# delegates to docker compose. Any arguments after --rebuild-clp are
# forwarded to `docker compose up`.
#
# Usage:
#   ./docker/start.sh [--rebuild-clp] [docker compose up args...]
#
# Options:
#   --rebuild-clp   Force a fresh build of the CLP core .deb package
#
# Examples:
#   ./docker/start.sh                   # start attached
#   ./docker/start.sh -d                # start detached
#   ./docker/start.sh --rebuild-clp -d  # rebuild CLP, then start detached
#   ./docker/start.sh --no-build        # skip image rebuild (use cached image)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

rebuild_clp=false
compose_args=()
for arg in "$@"; do
    if [[ "$arg" == "--rebuild-clp" ]]; then
        rebuild_clp=true
    else
        compose_args+=("$arg")
    fi
done

# Build clp-core .deb if not present or rebuild was requested.
# The .deb is installed inside the Docker image to provide the clp-s binary.
if [[ "$rebuild_clp" == true ]] || ! ls "$PROJECT_DIR"/clp-core_*.deb >/dev/null 2>&1; then
    "$PROJECT_DIR/scripts/build-clp.sh"
fi

docker compose -f "$SCRIPT_DIR/docker-compose.yml" up --build "${compose_args[@]}"
