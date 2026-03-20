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
2. `IngestionGrpcService` converts proto records to internal domain objects, delegates to `IngestionService`
3. `IngestionService` validates records and submits to `BatchingWriter` channel (dim/agg column resolution happens at batch flush time in the `tableWriter`)
4. Response sent to client (record accepted for async processing)
5. `tableWriter` batches records and UPSERTs to database asynchronously

**Backpressure:**

`Submit()` uses a non-blocking send on a buffered channel (size 5000). If the channel is full, the request is rejected immediately with `RESOURCE_EXHAUSTED` — the gRPC goroutine is freed and the client retries with backoff.

| Condition | gRPC status | Client action |
|-----------|-------------|---------------|
| Channel has space | `OK` (`IngestResponse.Accepted: true`) | Record queued for async batch write |
| Channel full | `RESOURCE_EXHAUSTED` | Retry with backoff |
| Request deadline passed | `DEADLINE_EXCEEDED` | Retry |
| Validation error | `INVALID_ARGUMENT` | Fix request |
| Internal error | `OK` (`IngestResponse.Accepted: false`) | Retry |

**Characteristics:**

- Runs on every node — no single-owner coordination needed, any node can write metadata for any table
- Sits behind a load balancer for horizontal scaling. **Sticky sessions are recommended** — routing requests for the same table to the same node improves batch efficiency (more records per batch, fewer DB round trips) and reduces active `tableWriter` goroutines across the cluster
- Low latency — each gRPC request submits 1 record, but the `tableWriter` drains multiple pending submits into a single database batch when they arrive faster than the flush interval

## Kafka (Pull) — Single-Threaded Drain with Offset Watermark

The client publishes metadata to a Kafka topic. The coordinator's Kafka Consumer polls messages and submits them to the `BatchingWriter` via `IngestWithCallbackWait()` (blocking — waits for channel space).

**Offset tracking:** The `Consumer` maintains a per-partition watermark — a map from each Kafka partition to the highest offset whose flush has been confirmed. This watermark is the basis for offset commits.

**Protocol:**

1. Consumer calls `Poll(100ms)` — returns immediately when records are available, then drains all buffered messages with non-blocking `Poll(0)` (up to 1000 per cycle)
2. Each message is transformed and submitted via `IngestWithCallbackWait()` with a per-record `Flushed chan error` (buffered, cap 1)
3. Pending flushes are tracked in a `pendingFlushes` slice — no goroutines, no mutex
4. Each poll cycle, `drainFlushes()` non-blockingly checks all pending flush channels via `select`/`default`
5. Completed flushes have their offsets queued in `pendingCommit`; `commitPending()` deduplicates and commits the highest offset+1 per partition

**Why single-threaded drain?** All consumer state (`pendingFlushes`, `lastOffsets`, `pendingCommit`) is owned by the poll goroutine. No mutex, no goroutine-per-message — the `select`/`default` pattern non-blockingly checks each `Flushed` channel. Records whose flush hasn't completed yet are simply retained for the next cycle.

**Backpressure:** The Kafka consumer uses `SubmitWait()` — a blocking channel send that waits until space opens or the context is cancelled. When the channel is full, the poll loop blocks, the consumer stops polling, and Kafka retains messages in the topic. No messages are dropped due to channel capacity. Sustained backpressure indicates the database is slower than the Kafka ingestion rate — scale the DB or reduce topic throughput.

**Characteristics:**

- Events persist in the topic regardless of client or coordinator availability
- Requires single-owner coordination — one Kafka consumer group per table, managed by the HA mechanism (see [Coordinator HA](../design/coordinator-ha.md))
- Higher throughput — batch-poll drains up to 1000 messages per cycle, reducing per-record overhead
- Deterministic consumer group ID (`clp-coordinator-{table_name}-{table_id}`) enables automatic resumption on failover — the UUID suffix ensures uniqueness across environments sharing the same Kafka cluster

## Choosing a Path

gRPC ingestion runs on every node behind a load balancer, so a single node failure is transparent to clients. The trade-off: gRPC requires the coordinator to be available at write time. If the metastore is unavailable, the Client SDK must buffer and re-send — and if the client process dies before re-sending, that metadata is lost.

The Kafka path decouples the client from the coordinator — events persist in the topic regardless of client or metastore availability. The trade-off: Kafka introduces an additional infrastructure dependency. If you already operate a reliable Kafka cluster, this may be acceptable.

| Factor | gRPC | Kafka |
|--------|------|-------|
| **Metadata durability** | Lost if metastore down AND client node dies before retry | Persists in topic regardless |
| **Failure points** | MariaDB/MySQL + this service | MariaDB/MySQL + this service + Kafka |
| **Operational complexity** | Lower — no additional infrastructure | Higher — Kafka cluster management |
| **Latency** | Lower — client receives ACK after commit | Slightly higher — commit-then-offset cycle |
| **Scaling model** | Any node (load-balanced, sticky sessions recommended) | Single owner per table (HA-managed) |

Both paths can run simultaneously. For most deployments, gRPC is the simpler starting point.

## See Also

- [Architecture Overview](overview.md) — System overview and data flow diagrams
- [Coordinator HA](../design/coordinator-ha.md) — Why Kafka ingestion requires single-owner coordination
- [Configuration](../reference/configuration.md) — gRPC and Kafka settings
