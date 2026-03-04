#!/bin/bash
# Wrapper for Google Java Format
# Auto-installs the jar and Java 21 on first run

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${TOOLS_DIR}/bin"
GJF_VERSION="1.27.0"
GJF_JAR="${BIN_DIR}/google-java-format-${GJF_VERSION}-all-deps.jar"
JAVA_DIR="${BIN_DIR}/jdk21"

# Auto-install Google Java Format if missing
if [ ! -f "${GJF_JAR}" ]; then
    echo "Installing Google Java Format ${GJF_VERSION}..."
    mkdir -p "${BIN_DIR}"
    curl -fL -o "${GJF_JAR}" \
        "https://github.com/google/google-java-format/releases/download/v${GJF_VERSION}/google-java-format-${GJF_VERSION}-all-deps.jar"
    echo "Installed: ${GJF_JAR}"
fi

# Resolve Java 21+ (required by gjf)
check_java_version() {
    "$1" -version 2>&1 | grep -qE "version \"(21|[2-9][0-9])\."
}

JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/java}"
JAVA_BIN="${JAVA_BIN:-java}"

if ! check_java_version "$JAVA_BIN" 2>/dev/null; then
    # Auto-install Java 21 if system Java is too old
    if [ ! -f "${JAVA_DIR}/bin/java" ] && [ ! -f "${JAVA_DIR}/Contents/Home/bin/java" ]; then
        echo "Downloading Java 21 for Google Java Format..."

        OS=$(uname | tr '[:upper:]' '[:lower:]')
        ARCH=$(uname -m)
        case "$ARCH" in
            x86_64) ARCH="x64" ;;
            aarch64|arm64) ARCH="aarch64" ;;
        esac
        [[ "$OS" == "darwin" ]] && OS="mac"

        JDK_VERSION="21.0.5+11"
        JDK_URL="https://github.com/adoptium/temurin21-binaries/releases/download/jdk-${JDK_VERSION}/OpenJDK21U-jdk_${ARCH}_${OS}_hotspot_${JDK_VERSION/+/_}.tar.gz"

        mkdir -p "${BIN_DIR}"
        curl -fL "${JDK_URL}" | tar -xz -C "${BIN_DIR}"
        mv "${BIN_DIR}"/jdk-* "${JAVA_DIR}"
        echo "Installed: ${JAVA_DIR}"
    fi

    JAVA_BIN="${JAVA_DIR}/bin/java"
    [ -f "${JAVA_DIR}/Contents/Home/bin/java" ] && JAVA_BIN="${JAVA_DIR}/Contents/Home/bin/java"
fi

# Format files
echo "${@}" | xargs "$JAVA_BIN" -jar "$GJF_JAR" \
    --skip-reflowing-long-strings \
    --replace
