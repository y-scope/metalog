# gRPC Ingestion Benchmark

End-to-end throughput benchmark for the gRPC ingestion path. Starts a MariaDB container,
produces a configurable number of metadata records via the `Ingest` RPC (one record per RPC,
many in-flight simultaneously), and reports two metrics: send throughput and DB flush latency.

## What it measures

- **gRPC send throughput** — records/sec while RPCs are in flight (producer + coordinator
  pipeline combined; coordinator ACKs only after the DB write completes)
- **DB flush time** — wall-clock time from last RPC sent to all records confirmed in DB
  (typically a few hundred milliseconds as the final batch drains)

## Prerequisites

- Docker (with Compose v2 or standalone `docker-compose`)
- JDK 17+
- Maven 3.9+

## Usage

```bash
# From the metalog directory
./integration-tests/benchmarks/grpc-ingestion/run.py
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-r`, `--records` | `100000` | Number of records to send |
| `-a`, `--apps` | `10` | Distinct application IDs |
| `-c`, `--concurrency` | `5000` | Max concurrent in-flight RPCs |
| `-t`, `--timeout` | `300` | Seconds to wait for DB to catch up |
| `-m`, `--mode` | `structured` | Wire format (see below) |
| `-s`, `--skip-infra` | false | Reuse already-running containers |
| `--skip-clean` | false | Skip dropping and recreating the table |

### Wire format modes

| Mode | Payload | BatchingWriter path |
|------|---------|---------------------|
| `structured` | `MetadataRecord` with typed `DimEntry`/`AggEntry` | NUL-separated composite key (no regex) |
| `compat` | `MetadataRecord` with `SelfDescribingEntry` `/`-keys | Self-describing key parsing |

### Examples

```bash
# Quick smoke test
./integration-tests/benchmarks/grpc-ingestion/run.py -r 5000

# High-throughput run
./integration-tests/benchmarks/grpc-ingestion/run.py -r 500000 -c 10000

# Test compat wire format
./integration-tests/benchmarks/grpc-ingestion/run.py -r 100000 --mode compat

# Reuse existing containers
./integration-tests/benchmarks/grpc-ingestion/run.py -r 100000 --skip-infra
```

## Port customization

Create a `.env` file in the project root to override default ports:

```ini
DB_PORT=3307
GRPC_PORT=9092
```

## Expected output

```
==========================================
  GRPC INGESTION BENCHMARK RESULTS
==========================================

  Status      : PASS
  Records     : 100000 / 100000
  Apps        : 10 distinct
  Concurrency : 5000 concurrent RPCs
  Mode        : structured
  ----------------------------------------
  gRPC send (producer + coordinator pipeline)
    Duration  : 12685 ms  (12.7 s)
    Throughput: 7883 rec/s
  ----------------------------------------
  DB flush (time for writes to commit)
    Duration  : 203 ms  (0.2 s)

==========================================
```

The coordinator ACKs each RPC only after the DB batch commits, so `gRPC send throughput`
directly reflects end-to-end write throughput. The short `DB flush` time shows the final
in-flight batch draining after the last RPC is sent.

See [Performance Tuning](../../../docs/operations/performance-tuning.md) for a comparison
with the Kafka ingestion path and an explanation of the throughput difference.
