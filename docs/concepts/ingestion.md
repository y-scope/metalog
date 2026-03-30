# Ingestion Paths

[← Back to docs](../README.md)

The Client SDK writes files to object storage and sends file metadata to the coordinator via gRPC. The gRPC path provides commit-then-ack semantics and submits to the `BatchingWriter`. Both validation and database persistence happen through the same batching and UPSERT logic.

Files can be IR (streaming/appendable) or Archives (columnar). For IR, the entry type determines whether the file is left as-is or destined for consolidation into an Archive. See [Data Lifecycle](overview.md#data-lifecycle) for the full state diagram.

## BatchingWriter

The gRPC ingestion path submits records to the `BatchingWriter`, which lazily creates one `tableWriter` goroutine per active table on first submit. Each `tableWriter` has its own buffered `chan *FileRecord`, so writes to different tables never block each other.

**Batching strategy:** A `tableWriter` flushes when either condition is met — whichever comes first:

- **Time:** 1 second elapses since the last flush (configurable via `DefaultBatchFlushInterval`)
- **Count:** the batch reaches the configured record limit (default 5000)

Under low volume, the time trigger ensures metadata is durably committed within roughly 1 second. Under high volume, the count trigger kicks in first, keeping batches full and throughput high.

**Flush notification:** Each `FileRecord` carries an optional `Flushed chan error` (buffered, cap 1). After a batch is durably committed (or fails), the `tableWriter` sends `nil` (success) or an error to each record's `Flushed` channel.

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

## See Also

- [Architecture Overview](overview.md) — System overview and data flow diagrams
- [Configuration](../reference/configuration.md) — gRPC settings
