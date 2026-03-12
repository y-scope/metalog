# Task Queue Scalability Benchmark

Sweeps a matrix of worker counts and batch sizes to find the throughput ceiling of the
task queue under concurrent load. Useful for capacity planning: how many workers are
needed for a given workload?

## What it measures

For each (worker count, batch size) combination:
- **Claim throughput** (tasks/sec) — `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE` in a READ COMMITTED transaction
- **Complete throughput** (tasks/sec) — `UPDATE state = 'completed'`

Effective throughput is the minimum of the two, since both must keep pace in production.

## Prerequisites

- Docker (with Compose v2 or standalone `docker-compose`)
- Go 1.25+

## Usage

```bash
# From the metalog directory
./integration-tests/benchmarks/task-queue-scalability/run.py
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-w`, `--workers` | `1,25,50,75,100,125` | Comma-separated worker counts to test |
| `-b`, `--batch` | `1,5,10` | Comma-separated batch sizes to test |
| `-t`, `--tasks` | `50` | Tasks per worker per run |
| `-T`, `--tables` | `10` | Number of tables (reflects typical multi-table deployment) |
| `-s`, `--skip-infra` | false | Reuse already-running MariaDB container |

### Examples

```bash
# Default matrix
./integration-tests/benchmarks/task-queue-scalability/run.py

# Single-table stress test (maximum lock contention)
./integration-tests/benchmarks/task-queue-scalability/run.py -T 1

# Focused run
./integration-tests/benchmarks/task-queue-scalability/run.py -w 50,100,200 -b 1,5
```

## Port customization

```ini
# .env in project root
DB_PORT=3307
```
