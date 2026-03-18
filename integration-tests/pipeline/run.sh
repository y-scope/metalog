#!/bin/bash
#
# Run the consolidation pipeline end-to-end test.
#
# Tests the full cycle: IR generation -> Planner -> Worker (clp-s) -> Archive.
# Uses testcontainers (MariaDB + MinIO) — no Docker Compose needed.
#
# Prerequisites:
#   - Docker (for testcontainers)
#   - clp-s in $PATH (auto-built if missing and scripts/build-clp.sh exists)
#
# Usage:
#   ./integration-tests/pipeline/run.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() { echo -e "${YELLOW}----${NC} $1"; }

# Check Docker
if ! docker info &>/dev/null; then
    echo -e "${RED}ERROR${NC}: Docker is not running" >&2
    exit 1
fi

# Check/build clp-s
if ! command -v clp-s &>/dev/null; then
    if [ -f "$PROJECT_DIR/scripts/build-clp.sh" ]; then
        info "clp-s not found in \$PATH, building..."
        "$PROJECT_DIR/scripts/build-clp.sh"
        export PATH="$PROJECT_DIR/scripts/out:$PATH"
    else
        echo -e "${RED}ERROR${NC}: clp-s not found in \$PATH and no build script available" >&2
        echo "Install clp-s from https://github.com/y-scope/clp or add it to \$PATH" >&2
        exit 1
    fi
fi

info "Using clp-s: $(command -v clp-s)"
info "Running pipeline test..."

go test -tags e2e ./integration-tests/pipeline/... -timeout 5m -v "$@"

echo ""
echo -e "${GREEN}Pipeline test passed!${NC}"
