# Tutorial: End-to-End Ingestion

[← Back to docs](../README.md)

Walk through the full Kafka ingestion pipeline. **Prerequisite:** Complete the [Quickstart](quickstart.md) first — the node must be running.

The service supports gRPC (commit-then-ack) and Kafka ingestion. This tutorial uses Kafka — see [Ingestion Paths](../concepts/ingestion.md) for details on both paths.

---

## 1. Create the Kafka topic

```bash
docker compose -f docker/docker-compose.yml exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic spark-ir \
  --partitions 1 \
  --replication-factor 1
```

## 2. Produce a test message

Each Kafka message represents one IR file's metadata. Produce a single record:

```bash
echo '{"ir_storage_backend":"s3","ir_bucket":"clp-ir","ir_path":"s3://clp-ir/app-001/executor-0/file-001.clp.zst","state":"IR_ARCHIVE_BUFFERING","min_timestamp":1704067200,"max_timestamp":1704067500,"record_count":1000,"counts":{"agg_int/gte/level/debug":900,"agg_int/gte/level/info":700,"agg_int/gte/level/warn":100,"agg_int/gte/level/error":10,"agg_int/gte/level/fatal":1},"raw_size_bytes":5242880,"ir_size_bytes":1048576,"retention_days":30,"expires_at":0}' \
  | docker compose -f docker/docker-compose.yml exec -T kafka kafka-console-producer \
      --bootstrap-server localhost:9092 \
      --topic spark-ir
```

> **Note:** `kafka-console-producer` treats each **line** as a separate message. The JSON must be on a single line.

## 3. Verify ingestion

The coordinator's Kafka poller consumes the message and the metadata writer batch-UPSERTs it to the `clp_spark` table. Wait a few seconds, then query:

```bash
docker compose -f docker/docker-compose.yml exec mariadb mariadb -uroot -ppassword metalog_metastore \
  -e "SELECT id, clp_ir_path, state, record_count FROM clp_spark;"
```

Expected output:

```
+----+---------------------------------------------------+----------------------+--------------+
| id | clp_ir_path                                       | state                | record_count |
+----+---------------------------------------------------+----------------------+--------------+
|  1 | s3://clp-ir/app-001/executor-0/file-001.clp.zst   | IR_ARCHIVE_BUFFERING |         1000 |
+----+---------------------------------------------------+----------------------+--------------+
```

## 4. Inspect the schema

The metadata table is created automatically with base columns plus any dimension/aggregation columns discovered from ingested records:

```bash
docker compose -f docker/docker-compose.yml exec mariadb mariadb -uroot -ppassword metalog_metastore \
  -e "DESCRIBE clp_spark;" | head -20
```

You'll see base columns (`id`, `clp_ir_path`, `state`, `min_timestamp`, ...) plus auto-added aggregation columns with opaque placeholder names (e.g., `agg_f01`, `agg_f02`). The logical mapping (which agg tracks `level=error`, etc.) is stored in `_agg_registry`.

## 5. Check coordinator logs

Watch the coordinator process the message:

```bash
docker compose -f docker/docker-compose.yml logs coordinator-node | grep -E "Batch|Claimed|Created unit"
```

You should see the coordinator claim the `clp_spark` table and log the batch-UPSERT.

## 6. Produce more records (optional)

Send additional records to see batching in action:

```bash
echo '{"ir_storage_backend":"s3","ir_bucket":"clp-ir","ir_path":"s3://clp-ir/app-001/executor-0/file-002.clp.zst","state":"IR_ARCHIVE_BUFFERING","min_timestamp":1704067200,"max_timestamp":1704067800,"record_count":2500,"counts":{"agg_int/gte/level/debug":2200,"agg_int/gte/level/info":1800,"agg_int/gte/level/warn":300,"agg_int/gte/level/error":25,"agg_int/gte/level/fatal":0},"raw_size_bytes":10485760,"ir_size_bytes":2097152,"retention_days":30,"expires_at":0}' \
  | docker compose -f docker/docker-compose.yml exec -T kafka kafka-console-producer \
      --bootstrap-server localhost:9092 \
      --topic spark-ir
```

Then verify:

```bash
docker compose -f docker/docker-compose.yml exec mariadb mariadb -uroot -ppassword metalog_metastore \
  -e "SELECT COUNT(*) AS rows, SUM(record_count) AS total_records FROM clp_spark;"
```

---

## What just happened?

The data flowed through the Kafka ingestion path:

```
You (Kafka producer)
  │
  ▼
Kafka topic: spark-ir
  │
  ▼
Coordinator: Kafka Poller (per-table) — consumed message
  │
  ▼
Node: BatchingWriter → TableWriter (per-table) — batch-UPSERT to clp_spark table
  │
  ▼
MariaDB: clp_spark table — queryable metadata
```

The gRPC ingestion path is similar but shorter: the gRPC client sends an `IngestBatch` request directly to the node, which validates, batches, and commits to the database — then acks the client.

In a production deployment, the cycle continues: the Planner goroutine creates consolidation tasks, workers claim and execute them (semantic enrichment, PII handling, CLP compression), and the resulting archives are stored in object storage. See [Architecture Overview](../concepts/overview.md) for the full data lifecycle.

---

## See Also

- [Quickstart](quickstart.md) — Prerequisites and setup
- [Architecture Overview](../concepts/overview.md) — System overview and data lifecycle
- [Ingestion Paths](../concepts/ingestion.md) — gRPC and Kafka protocols, BatchingWriter
- [Configure Tables](../guides/configure-tables.md) — Table registration and feature flags
- [Configuration Reference](../reference/configuration.md) — Full configuration reference
