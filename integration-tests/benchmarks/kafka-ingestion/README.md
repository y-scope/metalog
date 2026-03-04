# Kafka Ingestion Benchmark

End-to-end throughput benchmark for the Kafka ingestion path. Starts real Docker
infrastructure (MariaDB, Kafka, ZooKeeper), produces a configurable number of metadata
records, and measures how fast the coordinator consumes and commits them to the database.

## What it measures

- **Producer throughput** — records/sec from `BenchmarkProducer` to Kafka
- **Coordinator throughput** — records/sec from Kafka topic to MariaDB commit

## Prerequisites

- Docker (with Compose v2 or standalone `docker-compose`)
- JDK 17+
- Maven 3.9+

## Usage

```bash
# From the clp-service directory
./integration-tests/benchmarks/kafka-ingestion/run.py
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-r`, `--records` | `10000` | Number of records to produce |
| `-a`, `--apps` | `10` | Distinct application IDs (partition key diversity) |
| `-t`, `--timeout` | `120` | Seconds to wait for all records to appear in DB |
| `-m`, `--mode` | `json` | Wire format (see below) |
| `-s`, `--skip-infra` | false | Reuse already-running containers |
| `-c`, `--skip-clean` | false | Skip dropping and recreating the table |

### Wire format modes

| Mode | Payload | Consumer path |
|------|---------|---------------|
| `json` | JSON `IRFile` object | `objectMapper.readValue(bytes, IRFile.class)` |
| `proto-structured` | `MetadataRecord` with `DimEntry`/`AggEntry` | `ProtoConverter.toIRFile()` structured path |
| `proto-compat` | `MetadataRecord` with `SelfDescribingEntry` `/`-keys | `ProtoConverter.toIRFile()` compat path |

The coordinator auto-detects format per message (JSON starts with `{`; anything else
is treated as protobuf) — no coordinator config change is needed when switching modes.

### Examples

```bash
# Quick smoke test
./integration-tests/benchmarks/kafka-ingestion/run.py -r 1000

# Full run with proto-structured payloads
./integration-tests/benchmarks/kafka-ingestion/run.py -r 100000 --mode proto-structured

# Reuse existing containers (skip docker up/down)
./integration-tests/benchmarks/kafka-ingestion/run.py -r 50000 --skip-infra
```

## Port customization

Create a `.env` file in the project root to override default ports:

```ini
DB_PORT=3307
KAFKA_PORT=9093
```

## Expected output

```
==========================================
  INGESTION BENCHMARK RESULTS
==========================================

  Status      : PASS
  Records     : 10000 / 10000
  Apps        : 10 distinct
  Mode        : json
  ----------------------------------------
  Producer
    Duration  : 850 ms
    Throughput: 11764 rec/s
  ----------------------------------------
  Coordinator
    Duration  : 3200 ms  (3.2 s)
    Throughput: 3125 rec/s

==========================================
```

Coordinator throughput is bounded by database batch-UPSERT speed. See
[Performance Tuning](../../../docs/operations/performance-tuning.md) for context.
