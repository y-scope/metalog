# Configure Tables

[← Back to docs](../README.md)

How to register tables, manage feature flags, and query table configuration.

---

## API Registration (recommended for runtime changes)

Tables can be registered at runtime via the `AdminService.RegisterTable` gRPC RPC on port
9090 — no node restart required. The call is fully idempotent.

```bash
grpcurl -plaintext -d '{
  "table_name": "my_spark_logs",
  "kafka": {"topic": "spark-ir", "bootstrap_servers": "kafka:29092"}
}' localhost:9090 \
  com.yscope.metalog.coordinator.grpc.AdminService/RegisterTable
# → {"tableName":"my_spark_logs","created":true}

# Second call — idempotent
# → {"tableName":"my_spark_logs","created":false}
```

After the API call writes the `_table_assignment` row (with `node_id = NULL`), the coordinator's
existing periodic `reconcileUnits()` loop claims it on the next cycle (default: every 60 s).

All optional feature flags supported by the proto are also accepted:

```bash
grpcurl -plaintext -d '{
  "table_name": "my_spark_logs",
  "display_name": "Spark Logs",
  "kafka": {
    "topic": "spark-ir",
    "bootstrap_servers": "kafka:29092",
    "record_transformer": "spark"
  },
  "kafka_poller_enabled": true,
  "consolidation_enabled": false
}' localhost:9090 \
  com.yscope.metalog.coordinator.grpc.AdminService/RegisterTable
```

> **Note:** `retention_cleanup_enabled` is not exposed by the `RegisterTable` RPC and must be set directly via SQL on `_table_config` if needed.

See [gRPC API — AdminService](../reference/grpc-api.md#coordinatorservice) for full field reference.

---

## SQL Registration

Tables can also be registered directly via SQL:

```sql
INSERT INTO _table (table_name, display_name) VALUES ('spark', 'Spark Logs');
INSERT INTO _table_kafka (table_name, kafka_topic) VALUES ('spark', 'spark-ir');
INSERT INTO _table_config (table_name) VALUES ('spark');
INSERT INTO _table_assignment (table_name) VALUES ('spark');
```

---

## Registry Sub-Tables

| Table | Purpose |
|-------|---------|
| `_table` | Identity — `table_id` (UUID PK), `table_name` (UNIQUE), `display_name`, `active` |
| `_table_kafka` | Kafka routing — `kafka_bootstrap_servers`, `kafka_topic` (used for Kafka ingestion path) |
| `_table_config` | Feature flags as typed columns (see below) |
| `_table_assignment` | Node assignment — `node_id` (NULL = unassigned), `lease_expiry`, `node_assigned_at` |

The Kafka consumer group ID is derived as `clp-coordinator-{table_name}-{table_id}`. The UUID component ensures uniqueness across environments (e.g., prod and staging sharing the same Kafka cluster). When a table migrates to a new node, the new owner reuses the same group ID and Kafka resumes from the last committed offset. No offset storage in the database.

See [Coordinator HA Design](../design/coordinator-ha.md) for liveness, heartbeat, orphan detection, and failover mechanics built on `_table_assignment` and `_node_registry`.

See `schema/schema.sql` for the full DDL.

---

## Querying Config

```sql
-- All tables and their assignments
SELECT t.table_name, k.kafka_topic, t.active, a.node_id
FROM _table t
JOIN _table_kafka k ON t.table_name = k.table_name
JOIN _table_assignment a ON t.table_name = a.table_name;

-- Feature flags for a specific table
SELECT * FROM _table_config WHERE table_name = 'spark';
```

---

## Feature Flags

Each per-table coordinator goroutine can be individually enabled/disabled via `_table_config` columns:

| Feature | Column (`_table_config`) | Default | Description |
|---------|--------------------------|---------|-------------|
| Kafka Consumer | `kafka_poller_enabled` | true | Polls Kafka, submits to BatchingWriter |
| Metadata Writer | `metadata_writer_enabled` | true | Accepts and batch-UPSERTs ingested records |
| Consolidation | `consolidation_enabled` | true | Creates IR→Archive consolidation tasks |
| Retention Cleanup | `retention_cleanup_enabled` | true | Purges expired rows and storage objects |

---

## See Also

- [Architecture Overview](../concepts/overview.md) — System overview and startup sequence
- [Configuration Reference](../reference/configuration.md) — Full node.yaml and environment variable reference
- [Quickstart](../getting-started/quickstart.md) — Setup and first run
- [Deploy HA](../guides/deploy-ha.md) — Node assignment, liveness, and failover
- [Ingestion Paths](../concepts/ingestion.md) — gRPC vs Kafka ingestion
- [Write Transformers](write-transformers.md) — `recordTransformer` values
- [Schema Evolution](evolve-schema.md) — Online DDL for new columns
