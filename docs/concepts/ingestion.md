# Ingestion Paths

[← Back to docs](../README.md)

The Client SDK writes files to object storage and sends file metadata to the coordinator. Two ingestion paths are available — **gRPC** (push) and **Kafka** (pull) — with different durability and operational trade-offs. Both can run concurrently for the same table, submitting to the same `BatchingWriter`. Both paths converge on the same batching and UPSERT logic.

Files can be IR (streaming/appendable) or Archives (columnar). For IR, the entry type determines whether the file is left as-is or destined for consolidation into an Archive. See [Data Lifecycle](overview.md#data-lifecycle) for the full state diagram.

## BatchingWriter

Both ingestion paths submit records to a single `BatchingWriter`, which lazily creates one `tableWriter` goroutine per active table on first submit. Each `tableWriter` has its own buffered `chan *FileRecord`, so writes to different tables never block each other.

**Batching strategy:** A `tableWriter` flushes when either condition is met — whichever comes first:

- **Time:** 1 second elapses since the last flush (configurable via `DefaultBatchFlushInterval`)
- **Count:** the batch reaches the configured record limit (default 5000)

Both paths share this behavior. Under low volume, the time trigger ensures metadata is durably committed within roughly 1 second. Under high volume, the count trigger kicks in first, keeping batches full and throughput high.

**Flush notification:** Each `FileRecord` carries an optional `Flushed chan error` (buffered, cap 1). After a batch is durably committed (or fails), the `tableWriter` sends `nil` (success) or an error to each record's `Flushed` channel. The Kafka consumer uses this to know when offsets are safe to commit.

## gRPC (Push)

The client sends metadata directly to the coordinator via the `Ingest` RPC (`IngestRequest` → `IngestResponse`). The coordinator submits records to the `BatchingWriter` channel and returns immediately — the response indicates the record was accepted for processing, not that it has been committed to the database.

**Protocol:**

1. Client sends `IngestRequest` (one record per RPC call) with file metadata (IR or Archive)
2. `IngestionHandler` converts proto records to internal domain objects, delegates to `IngestionService`
3. `IngestionService` validates records and submits to `BatchingWriter` channel (dim/agg column resolution happens at batch flush time in the `tableWriter`)
4. Response sent to client (record accepted for async processing)
5. `tableWriter` batches records and UPSERTs to database asynchronously

**Backpressure:**

The `BatchingWriter` channel (capacity 5000 per table) is a bounded queue between gRPC handler goroutines and the `tableWriter` that flushes to the database. When the channel is full, the behavior depends on the `grpc.blockingIngestion` config:

**Blocking mode** (`blockingIngestion: true`, default):

The gRPC goroutine **parks** on the channel send until space opens or the client deadline expires. The Go scheduler removes the goroutine from the run queue — zero CPU consumed, no network traffic, no retry overhead. When the `tableWriter` flushes a batch, a slot opens and the scheduler wakes one of the parked goroutines. From the client's perspective, the RPC latency increases from ~1ms to ~50-100ms under load, but it always succeeds (unless the deadline fires).

This is ~2x higher throughput than non-blocking mode because there are no wasted gRPC round-trips for retries. Each request does exactly one network round-trip.

**Non-blocking mode** (`blockingIngestion: false`):

The gRPC goroutine returns `RESOURCE_EXHAUSTED` immediately when the channel is full. The client must retry with backoff. Each retry is a full gRPC round-trip (serialize → send → receive → deserialize → sleep → repeat), which adds significant overhead under high concurrency.

**Safety valve:** In both modes, the client's context deadline acts as the safety valve. Production clients should always set a deadline (e.g., 5-10s):

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
resp, err := client.Ingest(ctx, req)
```

If the channel is backed up beyond the deadline (e.g., database is unreachable), the goroutine unblocks with `DEADLINE_EXCEEDED` and the client can fail over to another node or shed load.

**Memory under pressure:** Blocked goroutines consume ~4-8KB each. 50,000 simultaneously blocked goroutines = ~400MB — manageable for a server with 4-8GB of RAM, and this extreme scenario only occurs when the database is completely unresponsive.

| Condition | gRPC status | Client action |
|-----------|-------------|---------------|
| Channel has space | `OK` (`IngestResponse.Accepted: true`) | Record queued for async batch write |
| Channel full (blocking mode) | *(RPC latency increases)* | Request completes when space opens |
| Channel full (non-blocking mode) | `RESOURCE_EXHAUSTED` | Retry with backoff |
| Client deadline passed | `DEADLINE_EXCEEDED` | Fail over or shed load |
| Validation error | `INVALID_ARGUMENT` | Fix request |
| Internal error | `OK` (`IngestResponse.Accepted: false`) | Retry |

**Choosing a mode:**

| Mode | Throughput | Best for |
|------|-----------|----------|
| **Blocking** (default) | ~19K rec/s | Internal services, trusted clients, high-throughput pipelines |
| **Non-blocking** | ~11K rec/s | Public-facing APIs, load balancers needing fast failure signals |

**Characteristics:**

- Runs on every node — no single-owner coordination needed, any node can write metadata for any table
- Sits behind a load balancer for horizontal scaling. **Sticky sessions are recommended** — routing requests for the same table to the same node improves batch efficiency (more records per batch, fewer DB round trips) and reduces active `tableWriter` goroutines across the cluster
- Low latency — each gRPC request submits 1 record, but the `tableWriter` drains multiple pending submits into a single database batch when they arrive faster than the flush interval

## Kafka (Pull) — Source-Based Assignment with Single-Threaded Drain

The client publishes metadata to a Kafka topic. A `KafkaIngestionUnit` polls messages and submits them to the shared `BatchingWriter` via `IngestWithCallbackWait()` (blocking — waits for channel space).

### Kafka sources

Kafka consumption is decoupled from table ownership. Each Kafka source is an independent row in `_kafka_source`, registered via the admin API:

```
_kafka_source:
  table_name | source_name | topic     | bootstrap_servers | consumer_group_id | required_env
  web_logs   | us-east     | web_logs  | kafka-us:9092     | grp-us            | REGION=us-east
  web_logs   | eu-west     | web_logs  | kafka-eu:9092     | grp-eu            | REGION=eu-west
```

Multiple sources can target the same table — each runs as its own `KafkaIngestionUnit` on a different node. This enables multi-region deployments where regional Kafka clusters ingest into a single global metastore table.

**Source assignment** is managed via `_kafka_assignment` (separate from `_table_assignment`). During reconciliation, each node:

1. Discovers unclaimed sources via `GetUnclaimedKafkaSources`
2. Filters by `required_env` — a node claims a source only if its environment variables match (e.g., `REGION=us-east`)
3. Claims matching sources via CAS (`UPDATE ... WHERE node_id IS NULL`)
4. Starts a `KafkaIngestionUnit` per claimed source

If a node crashes, its source leases expire and another eligible node claims them. See [Kafka Source Assignment](../design/kafka-source-assignment.md) for the full design.

### Consumer internals

Each `KafkaIngestionUnit` runs a single-threaded consumer with offset watermark tracking:

**Offset tracking:** The `Consumer` tracks pending flush confirmations in a `pendingFlushes` slice. Each poll cycle, `drainFlushes()` checks completed flushes non-blockingly and queues their offsets in `pendingCommit`. `commitPending()` deduplicates and commits the highest offset+1 per partition to Kafka.

**Protocol:**

1. Consumer calls `Poll(100ms)` — returns immediately when records are available, then drains all buffered messages with non-blocking `Poll(0)` (up to 1000 per cycle)
2. Each message is transformed and submitted via `IngestWithCallbackWait()` with a per-record `Flushed chan error` (buffered, cap 1)
3. Pending flushes are tracked in a `pendingFlushes` slice — no goroutines, no mutex
4. Each poll cycle, `drainFlushes()` non-blockingly checks all pending flush channels via `select`/`default`
5. Completed flushes have their offsets queued in `pendingCommit`; `commitPending()` deduplicates and commits the highest offset+1 per partition

**Why single-threaded drain?** All consumer state (`pendingFlushes`, `pendingCommit`) is owned by the poll goroutine. No mutex, no goroutine-per-message — the `select`/`default` pattern non-blockingly checks each `Flushed` channel. Records whose flush hasn't completed yet are simply retained for the next cycle.

**Backpressure:** The Kafka consumer uses `IngestWithCallbackWait()` — which blocks until the `BatchingWriter` channel has space or the context is cancelled. When the channel is full, the poll loop blocks, the consumer stops polling, and Kafka retains messages in the topic. No messages are dropped due to channel capacity. Sustained backpressure indicates the database is slower than the Kafka ingestion rate — scale the DB or reduce topic throughput.

**Characteristics:**

- Events persist in the topic regardless of client or coordinator availability
- One `KafkaIngestionUnit` per source — multiple sources can target the same table (one per region)
- Higher throughput — batch-poll drains up to 1000 messages per cycle, reducing per-record overhead
- Consumer group ID is set per source via the admin API — enables independent offset tracking per regional consumer

## Choosing a Path

gRPC ingestion runs on every node behind a load balancer, so a single node failure is transparent to clients. The trade-off: gRPC requires the coordinator to be available at write time. If the metastore is unavailable, the Client SDK must buffer and re-send — and if the client process dies before re-sending, that metadata is lost.

The Kafka path decouples the client from the coordinator — events persist in the topic regardless of client or metastore availability. The trade-off: Kafka introduces an additional infrastructure dependency. If you already operate a reliable Kafka cluster, this may be acceptable.

| Factor | gRPC | Kafka |
|--------|------|-------|
| **Metadata durability** | Lost if metastore down AND client node dies before retry | Persists in topic regardless |
| **Failure points** | MariaDB/MySQL + this service | MariaDB/MySQL + this service + Kafka |
| **Operational complexity** | Lower — no additional infrastructure | Higher — Kafka cluster management |
| **Latency** | Lower — client receives ACK after record is queued (async DB write) | Slightly higher — commit-then-offset cycle |
| **Scaling model** | Any node (load-balanced, sticky sessions recommended) | One owner per source (HA-managed, multi-region capable) |

Both paths can run simultaneously. For most deployments, gRPC is the simpler starting point.

## See Also

- [Architecture Overview](overview.md) — System overview and data flow diagrams
- [Kafka Source Assignment](../design/kafka-source-assignment.md) — Source-based assignment design and multi-region deployment
- [Coordinator HA](../design/coordinator-ha.md) — HA strategy for table and source ownership
- [Configuration](../reference/configuration.md) — gRPC and Kafka settings
