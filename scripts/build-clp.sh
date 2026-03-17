#!/bin/bash
set -euo pipefail

# Build script for CLP core binaries and .deb package
# Usage: ./scripts/build-clp.sh [OPTIONS]
#
# Options:
#   --repo URL      Git repository URL (default: https://github.com/y-scope/clp.git)
#   --branch NAME   Git branch to checkout (default: timestamp-compatibility-snapshot)
#   --cores N       Number of parallel build jobs (default: 4)
#   --version VER   Package version (default: extracted from taskfile.yaml)
#   --arch ARCH     Architectures to build: aarch64, x86_64, or "all" (default: host arch)
#   --output DIR    Output directory for .deb package (default: project root)
#   --clean         Remove build artifacts before building
#   --help          Show this help message

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SRC_DIR="/tmp/clp-build"

# Defaults
REPO_URL="https://github.com/y-scope/clp.git"
BRANCH="timestamp-compatibility-snapshot"
CORES=4
CLEAN=false
VERSION=""
OUTPUT_DIR="${PROJECT_DIR}"
TARGET_ARCHES=""

BINARIES=(clg clo clp clp-s indexer log-converter reducer-server)

# Apply cmake fix to resolve broken Python interpreter paths in manylinux base image
# Issue: manylinux_2_28 base image updated Feb 6, 2026 with cmake 4.2.1
#        CLP's install-all.sh uninstalls cmake 4.x and tries to reinstall 3.x,
#        but the reinstall fails silently, leaving broken symlinks
# Fix: Force reinstall cmake and uv using pipx to create working interpreters
apply_cmake_fix() {
    local source_image="$1"
    local target_image="$2"
    local platform="$3"

    echo "    Applying cmake fix to ${target_image}..."
    docker build --platform "$platform" -t "$target_image" - <<EOF
FROM ${source_image}
# Fix broken pipx packages from upstream install-all.sh
# The manylinux base image ships with cmake 4.x, but CLP needs 3.x.
# Upstream uninstalls cmake 4.x and attempts to reinstall 3.x, but this fails
# silently, leaving broken symlinks with invalid Python interpreter paths.
RUN pipx install --force 'cmake<4,>=3.23' && \\
    pipx install --force 'uv>=0.8' && \\
    cmake --version
EOF
}

# Auto-detect CA certificate bundle from common locations
detect_ca_bundle() {
    local locations=(
        # macOS
        "/etc/ssl/cert.pem"
        # Debian/Ubuntu
        "/etc/ssl/certs/ca-certificates.crt"
        # RHEL/CentOS/Fedora
        "/etc/pki/tls/certs/ca-bundle.crt"
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"
        # openSUSE
        "/etc/ssl/ca-bundle.pem"
        # Alpine
        "/etc/ssl/certs/ca-certificates.crt"
    )
    for loc in "${locations[@]}"; do
        if [[ -f "$loc" ]]; then
            echo "$loc"
            return 0
        fi
    done
    return 1
}

CA_BUNDLE=""
if CA_BUNDLE=$(detect_ca_bundle); then
    echo "==> Auto-detected CA bundle: $CA_BUNDLE"
else
    echo "==> No CA bundle found (will use container defaults)"
    CA_BUNDLE=""
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --repo)     REPO_URL="$2";  shift 2 ;;
        --branch)   BRANCH="$2";    shift 2 ;;
        --cores)    CORES="$2";     shift 2 ;;
        --version)  VERSION="$2";   shift 2 ;;
        --arch)     TARGET_ARCHES="$2"; shift 2 ;;
        --output)   OUTPUT_DIR="$2"; shift 2 ;;
        --clean)    CLEAN=true;     shift ;;
        --help)     sed -n '4,16p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *)          echo "Unknown option: $1"; echo "Use --help for usage information"; exit 1 ;;
    esac
done

# Resolve output dir to absolute path
OUTPUT_DIR="$(mkdir -p "$OUTPUT_DIR" && cd "$OUTPUT_DIR" && pwd)"

# Default to host architecture
if [[ -z "$TARGET_ARCHES" ]]; then
    case $(uname -m) in
        x86_64)        TARGET_ARCHES="x86_64" ;;
        aarch64|arm64) TARGET_ARCHES="aarch64" ;;
        *)             echo "ERROR: Unsupported host architecture: $(uname -m)"; exit 1 ;;
    esac
fi
[[ "$TARGET_ARCHES" == "all" ]] && TARGET_ARCHES="aarch64,x86_64"
IFS=',' read -ra ARCH_LIST <<< "$TARGET_ARCHES"

# Clean if requested
if [[ "$CLEAN" == "true" ]]; then
    echo "==> Cleaning build artifacts..."
    for arch in "${ARCH_LIST[@]}"; do
        rm -rf "/tmp/clp-build-$(echo "$arch" | xargs)"
    done
fi

# Clone or update source
if [[ ! -d "$SRC_DIR" ]]; then
    echo "==> Cloning CLP repository..."
    git clone --branch "$BRANCH" --single-branch --recurse-submodules "$REPO_URL" "$SRC_DIR"
elif [[ -d "$SRC_DIR/.git" ]]; then
    echo "==> Updating existing repository..."
    cd "$SRC_DIR"
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$CURRENT_BRANCH" != "$BRANCH" ]]; then
        echo "    Switching from $CURRENT_BRANCH to $BRANCH..."
        git fetch origin "$BRANCH"
        git checkout "$BRANCH"
    fi
    git pull --ff-only origin "$BRANCH" || echo "    (already up to date or diverged)"
    cd "$SCRIPT_DIR"
else
    echo "ERROR: $SRC_DIR exists but is not a git repository"
    exit 1
fi

# Extract version from taskfile.yaml if not provided
if [[ -z "$VERSION" ]]; then
    VERSION=$(grep 'G_PACKAGE_VERSION:' "$SRC_DIR/taskfile.yaml" | head -1 | sed 's/.*"\(.*\)".*/\1/')
    if [[ -z "$VERSION" ]]; then
        echo "ERROR: Could not extract version from taskfile.yaml and --version not provided"
        exit 1
    fi
fi

echo "==> CLP Build Configuration"
echo "    Repository: ${REPO_URL}"
echo "    Branch:     ${BRANCH}"
echo "    Version:    ${VERSION}"
echo "    Cores:      ${CORES}"
echo "    Binaries:   ${BINARIES[*]}"
echo "    Arches:     ${ARCH_LIST[*]}"
echo "    Output:     ${OUTPUT_DIR}"
echo ""

# Build for each target architecture
for TARGET_ARCH in "${ARCH_LIST[@]}"; do
    TARGET_ARCH=$(echo "$TARGET_ARCH" | xargs)

    case $TARGET_ARCH in
        x86_64)  DOCKER_SUFFIX="x86_64";  DOCKER_PLATFORM="linux/amd64"; DEB_ARCH="amd64" ;;
        aarch64) DOCKER_SUFFIX="aarch64"; DOCKER_PLATFORM="linux/arm64"; DEB_ARCH="arm64" ;;
        *)       echo "ERROR: Unsupported architecture: $TARGET_ARCH (use aarch64 or x86_64)"; exit 1 ;;
    esac

    IMAGE_NAME="clp-build-env-${DOCKER_SUFFIX}"
    ARCH_BUILD_DIR="/tmp/clp-build-${DOCKER_SUFFIX}"
    DEB_NAME="clp-core_${VERSION}_${DEB_ARCH}.deb"

    echo "========================================"
    echo "Building for ${TARGET_ARCH} (${DEB_ARCH})"
    echo "========================================"

    # Build Docker image from y-scope's Dockerfile + patchelf
    if ! docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
        echo "==> Building Docker image ${IMAGE_NAME}..."

        # Create a temporary build context with CA certs if available
        BUILD_CONTEXT_DIR=$(mktemp -d)

        # Copy the upstream Dockerfile
        UPSTREAM_DOCKERFILE="${SRC_DIR}/components/core/tools/docker-images/clp-env-base-manylinux_2_28-${DOCKER_SUFFIX}/Dockerfile"

        if [[ -n "$CA_BUNDLE" ]]; then
            echo "    Injecting CA certificates into build..."
            cp "$CA_BUNDLE" "$BUILD_CONTEXT_DIR/ca-certificates.crt"

            # Create wrapper Dockerfile that installs CA certs first
            cat > "$BUILD_CONTEXT_DIR/Dockerfile" <<DOCKERFILE
# Inject host CA certificates before any network operations
FROM quay.io/pypa/manylinux_2_28_${DOCKER_SUFFIX}:latest

# Install CA certificates - update system trust store AND set env vars for curl/pip
COPY ca-certificates.crt /etc/pki/ca-trust/source/anchors/host-ca-bundle.crt
RUN update-ca-trust extract && \\
    cat /etc/pki/ca-trust/source/anchors/host-ca-bundle.crt >> /etc/pki/tls/certs/ca-bundle.crt

# Set environment variables that curl, pip, and other tools use for CA verification
ENV SSL_CERT_FILE=/etc/pki/tls/certs/ca-bundle.crt
ENV CURL_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt
ENV REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt
ENV PIP_CERT=/etc/pki/tls/certs/ca-bundle.crt

# Disable SSL verification for dnf repos (corporate proxy intercepts SSL)
RUN sed -i 's/sslverify=1/sslverify=0/g' /etc/yum.repos.d/*.repo 2>/dev/null || true && \\
    echo "sslverify=0" >> /etc/dnf/dnf.conf

# Configure pip to trust our CA bundle (for all Python versions)
RUN mkdir -p /root/.config/pip && \\
    echo "[global]" > /root/.config/pip/pip.conf && \\
    echo "cert = /etc/pki/tls/certs/ca-bundle.crt" >> /root/.config/pip/pip.conf && \\
    echo "trusted-host = pypi.org" >> /root/.config/pip/pip.conf && \\
    echo "               pypi.python.org" >> /root/.config/pip/pip.conf && \\
    echo "               files.pythonhosted.org" >> /root/.config/pip/pip.conf

# Configure curl to skip SSL verification (corporate proxy intercepts SSL)
RUN echo "insecure" > /root/.curlrc

# Now run upstream install steps
WORKDIR /root
COPY tools/scripts/lib_install ./tools/scripts/lib_install

RUN pipx uninstall cmake
RUN pipx uninstall uv

RUN ./tools/scripts/lib_install/manylinux_2_28/install-all.sh

# Remove cached files
RUN dnf clean all && rm -rf /var/cache/dnf /tmp/* /var/tmp/*
DOCKERFILE

            # Copy tools/scripts/lib_install directory to build context
            mkdir -p "$BUILD_CONTEXT_DIR/tools/scripts"
            cp -r "${SRC_DIR}/components/core/tools/scripts/lib_install" "$BUILD_CONTEXT_DIR/tools/scripts/"

            docker build --platform "$DOCKER_PLATFORM" \
                -t "${IMAGE_NAME}-base-temp" \
                "$BUILD_CONTEXT_DIR"

            # Apply cmake fix on top of the CA-injected base image
            apply_cmake_fix "${IMAGE_NAME}-base-temp" "${IMAGE_NAME}-base" "$DOCKER_PLATFORM"

            # Clean up temporary image
            docker rmi "${IMAGE_NAME}-base-temp" 2>/dev/null || true
        else
            # No CA bundle - use upstream build.sh script
            echo "    Using upstream build.sh..."
            UPSTREAM_BUILD_SCRIPT="${SRC_DIR}/components/core/tools/docker-images/clp-env-base-manylinux_2_28-${DOCKER_SUFFIX}/build.sh"
            bash "$UPSTREAM_BUILD_SCRIPT"

            # Apply cmake fix on top of upstream image
            apply_cmake_fix \
                "clp-core-dependencies-${DOCKER_SUFFIX/x86_64/x86}-manylinux_2_28:dev" \
                "${IMAGE_NAME}-base" \
                "$DOCKER_PLATFORM"
        fi

        # Clean up temp directory
        rm -rf "$BUILD_CONTEXT_DIR"

        docker build --platform "$DOCKER_PLATFORM" -t "$IMAGE_NAME" - <<EOF
FROM ${IMAGE_NAME}-base
RUN yum install -y patchelf || dnf install -y patchelf || true
EOF
    fi

    mkdir -p "$ARCH_BUILD_DIR"

    echo "==> Starting build..."
    docker run --rm \
        --platform "$DOCKER_PLATFORM" \
        -v "${SRC_DIR}:/src:ro" \
        -v "${ARCH_BUILD_DIR}:/build" \
        -w /build \
        -e CORES="$CORES" \
        -e PKG_VERSION="$VERSION" \
        -e DEB_ARCH="$DEB_ARCH" \
        -e BINARIES="${BINARIES[*]}" \
        "$IMAGE_NAME" \
        bash -c '
            set -euo pipefail
            git config --global --add safe.directory "*"
            read -ra BINARIES <<< "$BINARIES"

            if [[ ! -f /build/taskfile.yaml ]]; then
                echo "==> Initializing build directory..."
                cp -a /src/. /build/
            fi

            echo "==> Building dependencies..."
            task deps:core

            echo "==> Configuring cmake..."
            mkdir -p build/core
            cd build/core
            cmake ../../components/core \
                -C /build/build/deps/cpp/cmake-settings/all-core.cmake \
                -DCMAKE_BUILD_TYPE=Release

            echo "==> Building all core binaries..."
            make -j"${CORES}" "${BINARIES[@]}"

            echo "==> Packaging .deb..."
            STAGING=/tmp/clp-deb
            rm -rf "$STAGING"
            mkdir -p "$STAGING/usr/bin" "$STAGING/usr/lib/clp" "$STAGING/DEBIAN"

            EXCLUDE_PATTERN="linux-vdso|ld-linux|libc\.so|libm\.so|libpthread|libdl|librt\.so|libresolv|libstdc\+\+|libgcc_s"

            echo "==> Collecting shared library dependencies..."
            for bin in "${BINARIES[@]}"; do
                BIN_PATH="/build/build/core/${bin}"
                [[ -f "$BIN_PATH" ]] || { echo "WARNING: ${bin} not found (skipping)"; continue; }

                ldd "$BIN_PATH" 2>/dev/null | while read -r line; do
                    LIB_PATH=$(echo "$line" | grep -oP "=> \K/[^ ]+" || true)
                    [[ -n "$LIB_PATH" ]] || continue
                    LIB_NAME=$(basename "$LIB_PATH")
                    echo "$LIB_NAME" | grep -qE "$EXCLUDE_PATTERN" && continue
                    if [[ ! -f "$STAGING/usr/lib/clp/$LIB_NAME" ]]; then
                        cp -L "$LIB_PATH" "$STAGING/usr/lib/clp/$LIB_NAME"
                        echo "    Bundled: $LIB_NAME"
                    fi
                done
            done

            echo "==> Patching bundled libraries..."
            for lib in "$STAGING/usr/lib/clp/"*.so*; do
                [[ -f "$lib" ]] || continue
                patchelf --set-rpath /usr/lib/clp "$lib" 2>/dev/null || true
            done

            echo "==> Installing binaries..."
            for bin in "${BINARIES[@]}"; do
                BIN_PATH="/build/build/core/${bin}"
                [[ -f "$BIN_PATH" ]] || continue
                cp "$BIN_PATH" "$STAGING/usr/bin/${bin}"
                patchelf --set-rpath /usr/lib/clp "$STAGING/usr/bin/${bin}"
                strip "$STAGING/usr/bin/${bin}"
                echo "    Installed: ${bin}"
            done

            cat > "$STAGING/DEBIAN/control" <<CTRL
Package: clp-core
Version: ${PKG_VERSION}
Architecture: ${DEB_ARCH}
Maintainer: YScope Inc. <support@yscope.com>
Depends: libc6 (>= 2.28), libstdc++6
Description: CLP core binaries for log compression and search
 Includes clp-s, clp, clo, clg, indexer, log-converter, and reducer-server.
CTRL

            echo "==> Building .deb package..."
            cd "$STAGING"
            tar czf /tmp/control.tar.gz -C "$STAGING/DEBIAN" .
            tar czf /tmp/data.tar.gz -C "$STAGING" --exclude=./DEBIAN .
            echo "2.0" > /tmp/debian-binary
            ar r "/build/clp-core_${PKG_VERSION}_${DEB_ARCH}.deb" \
                /tmp/debian-binary /tmp/control.tar.gz /tmp/data.tar.gz
        '

    cp "${ARCH_BUILD_DIR}/${DEB_NAME}" "${OUTPUT_DIR}/${DEB_NAME}"
    echo "==> Package: ${OUTPUT_DIR}/${DEB_NAME}"
    echo ""
done

echo "========================================"
echo "All builds complete!"
echo "========================================"
echo ""
ls -lh "${OUTPUT_DIR}"/clp-core_*.deb 2>/dev/null
echo ""
echo "Test:"
echo "  docker run --rm -v ${OUTPUT_DIR}:/debs debian:bookworm bash -c \\"
echo "    'dpkg -i /debs/clp-core_*.deb && clp-s --help'"
