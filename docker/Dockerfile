# Multi-stage build for Metalog (Go)
# Stage 1: Build
FROM golang:1.25-bookworm AS build
WORKDIR /build

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build all binaries
COPY . .
RUN CGO_ENABLED=1 go build -o /out/metalog ./cmd/metalog

# Stage 2: Runtime
FROM debian:bookworm-slim
WORKDIR /app

# Install runtime deps for confluent-kafka-go (librdkafka) and TLS
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# Install CLP core binaries from .deb (must be built first via scripts/build-clp.sh)
COPY clp-core_*.deb /tmp/
RUN dpkg -i /tmp/clp-core_*.deb || apt-get -f install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* /tmp/clp-core_*.deb

# Create staging/output dirs for CLP compression
RUN mkdir -p /tmp/clp-staging /tmp/clp-output

# Copy Go binary from build stage
COPY --from=build /out/metalog /app/metalog

# Default: run the unified metalog binary (config-driven behavior)
ENTRYPOINT ["/app/metalog", "serve"]
CMD ["--config", "/etc/clp/node.yaml"]
