# Configure Tables

[← Back to docs](../README.md)

How to register tables, manage feature flags, and query table configuration.

---

## API Registration (recommended for runtime changes)

Tables can be registered at runtime via the `CoordinatorService.RegisterTable` gRPC RPC on port
9090 — no node restart required. The call is fully idempotent.

```bash
grpcurl -plaintext -d '{
  "table_name": "my_spark_logs",
  "kafka": {"topic": "spark-ir", "bootstrap_servers": "kafka:29092"}
}' localhost:9090 \
  com.yscope.metalog.coordinator.grpc.CoordinatorService/RegisterTable
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
  "consolidation_enabled": false,
  "schema_evolution_enabled": true,
  "loop_interval_ms": 5000
}' localhost:9090 \
  com.yscope.metalog.coordinator.grpc.CoordinatorService/RegisterTable
```

> **Note:** `deletion_enabled` and `retention_cleanup_enabled` are not exposed by the `RegisterTable` RPC and must be set directly via SQL on `_table_config` if needed.

See [gRPC API — CoordinatorService](../reference/grpc-api.md#coordinatorservice) for full field reference.

---

## Declarative Registration (node.yaml)

Tables declared in `node.yaml` are auto-UPSERTed into the `_table*` registry on startup. The registry comprises 4 sub-tables with 1:1 relationships via `table_name`. Only explicitly specified fields are updated; omitted fields retain their DB defaults.

```yaml
tables:
  - name: clp_spark                  # required — metadata table name
    displayName: Spark Logs           # optional, defaults to name
    kafka:                            # required
      topic: spark-ir                 # required — Kafka topic to consume from
      bootstrapServers: kafka:29092   # optional, DB default: localhost:9092
      recordTransformer: spark        # optional — see Record Transformers doc
    # All feature flags below are optional. Omitted = use DB default.
    # kafkaPollerEnabled: true
    # consolidationEnabled: true
    # deletionEnabled: true
    # retentionCleanupEnabled: true
    # loopIntervalMs: 5000
```

See [Write Transformers](write-transformers.md) for the list of valid `recordTransformer` values and how they map ingested Kafka messages to IR file metadata.

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

See `core/src/main/resources/schema/schema.sql` for the full DDL.

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

Each per-table coordinator thread can be individually enabled/disabled via `_table_config` columns:

| Thread | Column (`_table_config`) | Default | Description |
|--------|--------------------------|---------|-------------|
| Kafka Poller | `kafka_poller_enabled` | true | Polls Kafka, fills the per-table event queue |
| Planner | `consolidation_enabled` | true | Creates IR→Archive consolidation tasks |
| Storage Deletion | `deletion_enabled` | true | Deletes files from object storage after state transitions |
| Retention Cleanup | `retention_cleanup_enabled` | true | DELETEs expired rows from the metadata table |

> **Note:** `metadata_writer_enabled` exists in `_table_config` but is not currently read by the coordinator. The BatchingWriter that drains the event queue is a node-level component shared across all tables and is not controlled per-table.

Partition management (`partition_manager_enabled`) also runs at the node level, not as a per-table thread. See [Metadata Schema](../concepts/metadata-schema.md#partitioning) and [Architecture Overview](../concepts/overview.md) for the full thread model.

> **Note:** `schemaEvolutionEnabled` is not a thread toggle — it controls whether the service automatically adds new `dim_*` and `agg_*` columns via online DDL when new fields are discovered. See [Schema Evolution](evolve-schema.md).

---

## See Also

- [Architecture Overview](../concepts/overview.md) — System overview and startup sequence
- [Configuration Reference](../reference/configuration.md) — Full node.yaml and environment variable reference
- [Quickstart](../getting-started/quickstart.md) — Setup and first run
- [Deploy HA](../guides/deploy-ha.md) — Node assignment, liveness, and failover
- [Ingestion Paths](../concepts/ingestion.md) — gRPC vs Kafka ingestion
- [Write Transformers](write-transformers.md) — `recordTransformer` values
- [Schema Evolution](evolve-schema.md) — Online DDL for new columns
