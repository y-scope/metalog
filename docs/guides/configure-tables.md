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
  "config_json": "{\"kafka\":{\"topic\":\"spark-ir\",\"bootstrap_servers\":\"kafka:29092\"}}"
}' localhost:9090 \
  com.yscope.metalog.coordinator.grpc.AdminService/RegisterTable
# → {"tableName":"my_spark_logs","created":true}

# Second call — idempotent
# → {"tableName":"my_spark_logs","created":false}
```

After the API call writes the `_table_assignment` row (with `node_id = NULL`), the coordinator's
existing periodic `reconcile()` loop claims it on the next cycle (default: every 60 s).

All per-table settings are passed via the `config_json` field — a JSON blob merged into the existing
config (read-modify-write). Unknown fields are rejected.

```bash
grpcurl -plaintext -d '{
  "table_name": "my_spark_logs",
  "display_name": "Spark Logs",
  "config_json": "{\"kafka\":{\"enabled\":true,\"topic\":\"spark-ir\",\"bootstrap_servers\":\"kafka:29092\",\"record_transformer\":\"spark\"},\"consolidation\":{\"enabled\":false}}"
}' localhost:9090 \
  com.yscope.metalog.coordinator.grpc.AdminService/RegisterTable
```

See [gRPC API — AdminService](../reference/grpc-api.md#coordinatorservice) for full field reference.

---

## SQL Registration

Tables can also be registered directly via SQL:

```sql
INSERT INTO _table (table_name, display_name) VALUES ('spark', 'Spark Logs');
INSERT INTO _table_config (table_name, config) VALUES ('spark',
  '{"kafka":{"topic":"spark-ir","bootstrap_servers":"kafka:29092"}}');
INSERT INTO _table_assignment (table_name) VALUES ('spark');
```

---

## Registry Sub-Tables

| Table | Purpose |
|-------|---------|
| `_table` | Identity — `table_id` (UUID PK), `table_name` (UNIQUE), `display_name`, `active` |
| `_table_config` | Unified JSON config blob — feature flags, Kafka routing, consolidation policies (NULL = all defaults) |
| `_table_assignment` | Node assignment — `node_id` (NULL = unassigned), `lease_expiry`, `node_assigned_at` |

The Kafka consumer group ID is derived as `clp-coordinator-{table_name}-{table_id}`. The UUID component ensures uniqueness across environments (e.g., prod and staging sharing the same Kafka cluster). When a table migrates to a new node, the new owner reuses the same group ID and Kafka resumes from the last committed offset. No offset storage in the database.

See [Coordinator HA Design](../design/coordinator-ha.md) for liveness, heartbeat, orphan detection, and failover mechanics built on `_table_assignment` and `_node_registry`.

See `pkg/ddl/schema.sql` for the full DDL.

---

## Querying Config

```sql
-- All tables and their assignments
SELECT t.table_name, t.active, a.node_id, c.config
FROM _table t
JOIN _table_config c ON t.table_name = c.table_name
JOIN _table_assignment a ON t.table_name = a.table_name;

-- Config for a specific table
SELECT * FROM _table_config WHERE table_name = 'spark';
```

---

## Config Schema

All per-table settings live in the `config` JSON blob in `_table_config`. A NULL blob means all
defaults apply. The config is organized by subsystem — each subsystem owns its `enabled` flag and
its settings under a single key.

### Full example

```json
{
  "kafka": {
    "enabled": true,
    "topic": "spark-ir",
    "bootstrap_servers": "kafka:29092",
    "record_transformer": "spark"
  },
  "consolidation": {
    "enabled": true,
    "policies": [
      { "type": "time_window", "config": {"window_size": "1h", "min_files": 2, "max_files": 100} }
    ]
  },
  "retention": {
    "enabled": true,
    "type": "default"
  }
}
```

### `kafka` — Kafka consumer routing

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable/disable Kafka consumer goroutine |
| `topic` | string | `""` | Kafka topic to consume from |
| `bootstrap_servers` | string | `""` | Kafka broker address(es) |
| `record_transformer` | string | `""` | Named record transformer (empty = default). See [Write Transformers](write-transformers.md). |

Even when `enabled` is `true`, the consumer only starts if `topic` and `bootstrap_servers` are both
non-empty. This lets you enable Kafka in advance and configure routing later.

### `consolidation` — IR-to-archive consolidation

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable/disable consolidation planner |
| `policies` | array | `[]` | Ordered list of consolidation policies (waterfall). If empty, a default `time_window(1h)` policy is used. |
| `stale_buffering_mins` | int | `60` | Minutes before an `IR_ARCHIVE_BUFFERING` file is auto-promoted to `CONSOLIDATION_PENDING`. Negative value disables. See [Stuck-File Promotion](../concepts/consolidation.md#stuck-file-promotion). |

Each policy in the `policies` array has a `type` and a policy-specific `config` object:

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Policy type: `"time_window"`, `"spark_job"` |
| `config` | object | Policy-specific parameters (see below) |

**`time_window` config:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `window_size` | string | `"1h"` | Time window duration, e.g. `"1h"`, `"30m"` |
| `min_files` | int | `2` | Minimum files to trigger consolidation |
| `max_files` | int | `100` | Maximum files per consolidation task |

**`spark_job` config:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `grouping_dim_key` | string | *(required)* | Dimension key to group by (e.g. `"application_id"`) |
| `min_files` | int | `2` | Minimum files to trigger consolidation |
| `max_files` | int | `100` | Maximum files per consolidation task |
| `job_timeout` | string | `"24h"` | Timeout before forcing consolidation of incomplete groups |

### `retention` — Lifecycle and expiration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable/disable retention cleanup |
| `type` | string | `"default"` | Retention strategy type. Custom strategies can be registered at compile time. |

---

## See Also

- [Architecture Overview](../concepts/overview.md) — System overview and startup sequence
- [Configuration Reference](../reference/configuration.md) — Full node.yaml and environment variable reference
- [Quickstart](../getting-started/quickstart.md) — Setup and first run
- [Deploy HA](../guides/deploy-ha.md) — Node assignment, liveness, and failover
- [Ingestion Paths](../concepts/ingestion.md) — gRPC vs Kafka ingestion
- [Write Transformers](write-transformers.md) — `recordTransformer` values
- [Schema Evolution](evolve-schema.md) — Online DDL for new columns
