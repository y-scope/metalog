#!/bin/bash
# Format all Java files in the project

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${REPO_ROOT}"

JAVA_FILES=$(find src -name "*.java" 2>/dev/null || true)
if [ -n "$JAVA_FILES" ]; then
    JAVA_COUNT=$(echo "$JAVA_FILES" | wc -l)
    echo "Formatting ${JAVA_COUNT} Java files..."
    echo "$JAVA_FILES" | xargs ./tools/gjf.sh
    echo "Done."
else
    echo "No Java files found"
fi
