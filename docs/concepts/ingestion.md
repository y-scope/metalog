# Ingestion Paths

[← Back to docs](../README.md)

The Client SDK writes files to object storage and sends file metadata to the coordinator. Two ingestion paths are available — **gRPC** (push) and **Kafka** (pull) — with different durability and operational trade-offs. Both can run concurrently for the same table, each using a separate `BatchingWriter` instance so they never contend. Both paths converge on the same batching and UPSERT logic.

Files can be IR (streaming/appendable) or Archives (columnar). For IR, the entry type determines whether the file is left as-is or destined for consolidation into an Archive. See [Data Lifecycle](overview.md#data-lifecycle) for the full state diagram.

## BatchingWriter

Both ingestion paths submit records to a `BatchingWriter`, which lazily creates one `TableWriter` thread per active table on first submit. Each `TableWriter` has its own `ArrayBlockingQueue` and dedicated thread, so writes to different tables never block each other.

**Batching strategy:** A `TableWriter` flushes when either condition is met — whichever comes first:

- **Time:** 1 second elapses since the last flush (poll timeout)
- **Count:** the batch reaches the configured record limit

Both paths share this behavior. Under low volume, the time trigger ensures metadata is durably committed within roughly 1 second. Under high volume, the count trigger kicks in first, keeping batches full and throughput high.

**Two instances per node** (see [Configuration](../reference/configuration.md)):

| Instance | Records per batch | Max queued submissions | Flush timeout | Submit mode |
|----------|-------------------|------------------------|---------------|-------------|
| Kafka | 500 | 5,000 | 1s | Blocking (`submitBlocking`, configurable timeout, default: 60s) |
| gRPC | 1,000 | 10,000 | 1s | Non-blocking (`submit`, immediate `QueueFullException`) |

All three settings — records per batch, max queued submissions, and flush timeout — are configurable per instance. The queue holds pending `submit()` calls (one call per Kafka poll batch, one call per gRPC record). With defaults, the Kafka queue holds up to 5,000 pending batches (each containing up to 500 records); the gRPC queue holds up to 10,000 pending submissions (each containing 1 record).

Callers need to know when their records are durably committed. Each `PendingBatch` carries a `CompletableFuture<WriteResult>` that completes after the database write succeeds. This future is the coordination point: Kafka uses it to trigger offset commits, and gRPC uses it to send the client acknowledgment.

## gRPC (Push) — Commit-Then-Ack

The client sends metadata directly to the coordinator via the `Ingest` RPC (`IngestRequest` → `IngestResponse`). The coordinator submits records to the gRPC `BatchingWriter` and acknowledges only after the database commit succeeds.

**Protocol:**

1. Client sends `IngestRequest` (one record per RPC call) with file metadata (IR or Archive)
2. `IngestionGrpcService` converts proto records to internal domain objects, delegates to `IngestionService`
3. `IngestionService` validates records, non-blocking `submit()` to the gRPC `BatchingWriter` — returns a `CompletableFuture` immediately
4. `TableWriter` batches records and UPSERTs to database
5. On future completion, the `whenComplete()` callback sends the gRPC response — ACK to client

The client does not receive an acknowledgment until the batch is durably committed. If the coordinator is unavailable or the queue is full, the client receives an error and can retry. The coordinator imposes a 10-second write timeout on the `CompletableFuture`; if the database write does not complete within that window, the RPC returns `DEADLINE_EXCEEDED`.

**Backpressure:**

| Condition | gRPC status | Client action |
|-----------|-------------|---------------|
| Queue full | `RESOURCE_EXHAUSTED` | Retry with backoff |
| Write timeout (10s) | `DEADLINE_EXCEEDED` | Retry |
| Internal error | `INTERNAL` | Retry |

**Characteristics:**

- Runs on every node — no single-owner coordination needed, any node can write metadata for any table
- Sits behind a load balancer for horizontal scaling. **Sticky sessions are recommended** — routing requests for the same table to the same node improves batch efficiency (more records per batch, fewer DB round trips) and reduces active `TableWriter` threads across the cluster
- Low latency — each gRPC request submits 1 record, but the `TableWriter` drains multiple pending submits into a single database batch when they arrive faster than the flush interval

## Kafka (Pull) — Per-Partition Offset Watermark

The client publishes metadata to a Kafka topic. The coordinator's Kafka Poller thread consumes messages and submits them to the Kafka `BatchingWriter` via `submitBlocking()`.

**Offset tracking:** The `MetadataConsumer` maintains a per-partition watermark — a map from each Kafka partition to the highest offset successfully polled. This watermark is the basis for offset commits.

**Protocol:**

1. Poller calls `consumer.poll(5s)` — returns immediately when records are available; 5 seconds is the max idle wait. Each record carries `sourcePartition` and `sourceOffset`
2. Poller calls `batchingWriter.submitBlocking(table, files)` — blocks until queue has space (configurable timeout, default: 60s)
3. `TableWriter` batches records and UPSERTs to database
4. On future completion, a callback extracts the maximum offset per partition from the written records and stages them for commit via `commitOffsets(Map<Integer, Long>)`
5. On the next `poll()`, `commitPendingOffset()` runs first — calls `consumer.commitSync()` with offset+1 per partition (so the next poll starts at the next unconsumed message)

**Why per-partition watermarks?** Kafka topics are partitioned. If a single global offset were tracked, committing it would skip uncommitted records from other partitions. Per-partition tracking ensures each partition advances independently, and `Math.max()` merging handles out-of-order future completions safely.

**Why commit at start of poll?** The pending offsets are set by future callbacks (which run on the `TableWriter` thread) via a thread-safe `AtomicReference`. Committing at the start of the next `poll()` ensures commits happen on the consumer thread, matching Kafka's single-threaded consumer model.

**Backpressure:** When the `BatchingWriter`'s per-table queue is full, `submitBlocking()` blocks the poller (configurable timeout, default: 60s), naturally throttling consumption. This propagates backpressure all the way to Kafka — the consumer stops polling, and Kafka retains messages in the topic.

**Characteristics:**

- Events persist in the topic regardless of client or coordinator availability
- Requires single-owner coordination — one Kafka consumer group per table, managed by the HA mechanism (see [Coordinator HA](../design/coordinator-ha.md))
- Higher throughput — larger batch sizes reduce per-record database overhead
- Deterministic consumer group ID (`clp-coordinator-{table_name}-{table_id}`) enables automatic resumption on failover

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
