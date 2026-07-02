# metalog

CLP Metastore Service — file-level metadata catalog for
[CLP](https://github.com/y-scope/clp). Ingests, queries, and manages
lifecycle of compressed log file metadata at petabyte scale.

## Architecture

- **MariaDB/MySQL** as single source of truth (no ZooKeeper/etcd)
- **gRPC** ingestion (`Ingest` and `BatchIngest`) and query APIs
- **Pluggable storage** backends (S3, filesystem, HTTP)
- **Daily RANGE partitions** on `min_timestamp` for write locality and query pruning

### Community Edition

Single-node metadata catalog: gRPC ingestion, dimension-based queries,
partition management.

### Enterprise Edition (premium crates)

| Feature | Crate | Description |
|---------|-------|-------------|
| Aggregations | `metalog-aggs` | Pre-computed per-file counts for early termination |
| Sketches | `metalog-sketches` | Bloom filter pruning (SBBF, Parquet-compatible) |
| Kafka | `metalog-kafka` | Pull-based ingestion with offset watermark tracking |
| HA | `metalog-ha` | Multi-node coordination, fair-share table claiming |
| Consolidation | `metalog-consolidation` | IR→Archive and File→Archive pipelines with task queue |
| Retention | `metalog-retention` | Policy-based deletion with per-file adjustment |

## Building

```bash
cargo build --release
```

## Usage

```bash
# Start the server
metalog serve --config node.yaml

# Register a table
metalog admin register-table --table web_logs
```

## Development

```bash
# Format (requires nightly)
cargo +nightly fmt --all

# Lint
cargo +nightly clippy --all-targets --all-features -- -D warnings

# Test
cargo test
```

## Workspace Structure

```
crates/
├── metalog-types/          # Core types (FileRecord, FileState, processor traits)
├── metalog-encoding/       # LZ4+msgpack codec
├── metalog-timeutil/       # Epoch nanosecond utilities
├── metalog-logutil/        # Throttled failure logging
├── metalog-config/         # YAML config with env overrides
├── metalog-db/             # MySQL connection pool, dialect detection, helpers
├── metalog-metastore/      # File records, advisory locks, UPSERT builder
├── metalog-schema/         # Column registry, partition manager, DDL
├── metalog-proto/          # Protobuf definitions (tonic/prost)
├── metalog-ingestion/      # BatchingWriter, BatchIngest service, proto conversion
├── metalog-coordinator/    # Progress tracking, table registration
├── metalog-query/          # Filter validation, keyset pagination, cache
├── metalog-storage/        # Backend trait, filesystem implementation
├── metalog-grpc/           # gRPC server and handlers
├── metalog-health/         # HTTP health probes
├── metalog-telemetry/      # OpenTelemetry metrics
├── metalog-node/           # Node orchestrator, coordinator units
├── metalog-bin/            # CLI binary (enterprise edition)
└── (premium crates)        # metalog-aggs, sketches, kafka, ha, consolidation, retention
```

## License

Apache-2.0
