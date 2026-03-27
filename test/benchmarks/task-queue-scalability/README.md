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

- Docker (for testcontainers)
- Go 1.25+

## Usage

```bash
# From the metalog directory
go run .
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--nodes` | `1,2,4,8,16,32` | CSV of node counts |
| `--workers-per-node` | `1,2,4,8,16,32` | CSV of workers-per-node counts |
| `--batch-multiplier` | `1` | batch = workers-per-node * multiplier |
| `--tasks-per-worker` | `50` | Tasks per worker goroutine |
| `--tables` | `10` | Number of tables to spread tasks across |
| `--pool-size` | `200` | DB connection pool size |

### Examples

```bash
# Default matrix
go run .

# Single-table stress test (maximum lock contention)
go run . --tables 1

# Focused run
go run . --nodes 4,8,16 --workers-per-node 4,8
```
