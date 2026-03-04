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

**Flush notification:** Each `FileRecord` carries an optional `Flushed chan error` (buffered, cap 1). After a batch is durably committed (or fails), the `tableWriter` sends `nil` (success) or an error to each record's `Flushed` channel. This is the coordination point: the Kafka consumer uses it to know when offsets are safe to commit, and gRPC uses it to send client acknowledgments.

## gRPC (Push) — Commit-Then-Ack

The client sends metadata directly to the coordinator via the `Ingest` RPC (`IngestRequest` → `IngestResponse`). The coordinator submits records to the `BatchingWriter` and acknowledges only after the database commit succeeds.

**Protocol:**

1. Client sends `IngestRequest` (one record per RPC call) with file metadata (IR or Archive)
2. `IngestionGrpcService` converts proto records to internal domain objects, delegates to `IngestionService`
3. `IngestionService` validates records, resolves dims/aggs via `ColumnRegistry`, submits to `BatchingWriter`
4. `tableWriter` batches records and UPSERTs to database
5. On flush completion, the `Flushed` channel signals success — gRPC handler sends the response

The client does not receive an acknowledgment until the batch is durably committed. If the coordinator is unavailable or the channel is full, the client receives an error and can retry.

**Backpressure:**

| Condition | gRPC status | Client action |
|-----------|-------------|---------------|
| Channel full | `RESOURCE_EXHAUSTED` | Retry with backoff |
| Write timeout | `DEADLINE_EXCEEDED` | Retry |
| Internal error | `INTERNAL` | Retry |

**Characteristics:**

- Runs on every node — no single-owner coordination needed, any node can write metadata for any table
- Sits behind a load balancer for horizontal scaling. **Sticky sessions are recommended** — routing requests for the same table to the same node improves batch efficiency (more records per batch, fewer DB round trips) and reduces active `tableWriter` goroutines across the cluster
- Low latency — each gRPC request submits 1 record, but the `tableWriter` drains multiple pending submits into a single database batch when they arrive faster than the flush interval

## Kafka (Pull) — Single-Threaded Drain with Offset Watermark

The client publishes metadata to a Kafka topic. The coordinator's Kafka Consumer polls messages and submits them to the `BatchingWriter` via `IngestWithCallback()`.

**Offset tracking:** The `Consumer` maintains a per-partition watermark — a map from each Kafka partition to the highest offset whose flush has been confirmed. This watermark is the basis for offset commits.

**Protocol:**

1. Consumer calls `Poll(100ms)` — returns immediately when records are available, then drains all buffered messages with non-blocking `Poll(0)` (up to 1000 per cycle)
2. Each message is transformed and submitted via `IngestWithCallback()` with a per-record `Flushed chan error` (buffered, cap 1)
3. Pending flushes are tracked in a `pendingFlushes` slice — no goroutines, no mutex
4. Each poll cycle, `drainFlushes()` non-blockingly checks all pending flush channels via `select`/`default`
5. Completed flushes have their offsets queued in `pendingCommit`; `commitPending()` deduplicates and commits the highest offset+1 per partition

**Why single-threaded drain?** All consumer state (`pendingFlushes`, `lastOffsets`, `pendingCommit`) is owned by the poll goroutine. No mutex, no goroutine-per-message — the `select`/`default` pattern non-blockingly checks each `Flushed` channel. Records whose flush hasn't completed yet are simply retained for the next cycle.

**Backpressure:** When the `BatchingWriter`'s per-table channel is full, the `Submit()` call blocks (context-aware), naturally throttling consumption. This propagates backpressure all the way to Kafka — the consumer stops polling, and Kafka retains messages in the topic.

**Characteristics:**

- Events persist in the topic regardless of client or coordinator availability
- Requires single-owner coordination — one Kafka consumer group per table, managed by the HA mechanism (see [Coordinator HA](../design/coordinator-ha.md))
- Higher throughput — batch-poll drains up to 1000 messages per cycle, reducing per-record overhead
- Deterministic consumer group ID (`metalog-coordinator-{table_name}`) enables automatic resumption on failover

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
