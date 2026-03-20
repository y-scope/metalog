# Ingestion Benchmark

Measures metadata ingestion throughput in three modes:
- **grpc** — send records via gRPC `Ingest` RPC
- **kafka-proto** — produce protobuf records to Kafka, measure consumer→DB throughput
- **kafka-json** — produce JSON records to Kafka, measure consumer→DB throughput

Infrastructure (MariaDB, Kafka) is started automatically via testcontainers and the
coordinator runs in-process. No manual setup needed.

## Prerequisites

- Docker (for testcontainers)
- Go 1.25+ with CGO enabled (required by confluent-kafka-go)

## Usage

```bash
# gRPC mode (default)
go run .

# Kafka protobuf mode
go run . --mode kafka-proto

# Kafka JSON mode
go run . --mode kafka-json
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--mode` | `grpc` | Benchmark mode: `grpc`, `kafka-proto`, `kafka-json` |
| `--records` | `100000` | Number of records to send |
| `--apps` | `10000` | Number of distinct app IDs |
| `--table` | `clp_spark` | Target table name |
| `--concurrency` | `5000` | Max concurrent in-flight RPCs (grpc mode) |
| `--batch-size` | `1000` | Kafka producer batch size |
| `--partitions` | `2` | Kafka topic partitions |
| `--timeout` | `120` | Timeout in seconds for DB convergence |

### Examples

```bash
# Quick gRPC test with fewer records
go run . --records 10000

# High-concurrency gRPC
go run . --concurrency 10000 --records 500000

# Kafka with more partitions
go run . --mode kafka-proto --partitions 4 --records 50000
```
