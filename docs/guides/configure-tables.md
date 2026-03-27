# Configure Tables

[← Back to docs](../README.md)

How to register tables, manage feature flags, and query table configuration.

---

## API Registration (recommended for runtime changes)

Tables can be registered at runtime via the `AdminService.RegisterTable` gRPC RPC on port
9090 — no node restart required. The call is fully idempotent.

```bash
grpcurl -plaintext -d '{
  "table_name": "my_spark_logs"
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
  "config_json": "{\"consolidation\":{\"enabled\":false}}"
}' localhost:9090 \
  com.yscope.metalog.coordinator.grpc.AdminService/RegisterTable
```

> **Note:** Kafka sources are registered separately via `AdminService.RegisterKafkaSource`.
> See [gRPC API — AdminService](../reference/grpc-api.md#registerkafkasource) for details.

See [gRPC API — AdminService](../reference/grpc-api.md#adminservice) for full field reference.

---

## SQL Registration

Tables can also be registered directly via SQL:

```sql
INSERT INTO _table (table_name, display_name) VALUES ('spark', 'Spark Logs');
INSERT INTO _table_config (table_name, config) VALUES ('spark', '{}');
INSERT INTO _table_assignment (table_name) VALUES ('spark');

-- Register a Kafka source separately
INSERT INTO _kafka_source (table_name, source_name, topic, bootstrap_servers)
VALUES ('spark', 'spark-main', 'spark-ir', 'kafka:29092');
```

---

## Registry Sub-Tables

| Table | Purpose |
|-------|---------|
| `_table` | Identity — `table_id` (UUID PK), `table_name` (UNIQUE), `display_name`, `active` |
| `_table_config` | Unified JSON config blob — feature flags, consolidation policies (NULL = all defaults) |
| `_kafka_source` | Kafka source definitions — one row per source (topic, bootstrap servers, transformer, env match) |
| `_kafka_assignment` | Kafka source-to-node assignment — tracks which node owns each source |
| `_table_assignment` | Node assignment — `node_id` (NULL = unassigned), `lease_expiry`, `node_assigned_at` |

The Kafka consumer group ID is derived from the `consumer_group_id` field in `_kafka_source` . When a source migrates to a new node, the new owner reuses the same group ID and Kafka resumes from the last committed offset. No offset storage in the database.

See [Coordinator HA Design](../design/coordinator-ha.md) for liveness, heartbeat, orphan detection, and failover mechanics built on `_table_assignment` and `_node_registry`.

See `schema/schema.sql` for the full DDL.

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

## Default Table Config

When `_table_config.config` is NULL (no explicit config stored), `DefaultTableConfig()` applies. This enables all subsystems with safe defaults:

```json
{
  "consolidation": { "enabled": true },
  "retention":     { "enabled": true, "type": "default" }
}
```

| Subsystem | Default `enabled` | Notes |
|-----------|:-----------------:|-------|
| `consolidation` | `true` | Uses default `time_window(1h)` policy when `policies` array is empty |
| `retention` | `true` | Strategy type `"default"` |

Kafka sources are managed independently via `_kafka_source` rows and the `RegisterKafkaSource` RPC — they are not part of `_table_config`.

A NULL config is functionally identical to storing the JSON above — `DecodeTableConfig(nil)` returns `DefaultTableConfig()`. To disable a subsystem, store an explicit config with `"enabled": false`.

---

## Config Schema

All per-table settings live in the `config` JSON blob in `_table_config`. A NULL blob means all
defaults apply. The config is organized by subsystem — each subsystem owns its `enabled` flag and
its settings under a single key.

### Full example

```json
{
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

> **Kafka sources** are no longer configured here. They are registered as independent
> entities in `_kafka_source` via the `AdminService.RegisterKafkaSource` RPC. Each source
> specifies its own `topic`, `bootstrap_servers`, `record_transformer`, `consumer_group_id`, and
> `required_env`. See [gRPC API — RegisterKafkaSource](../reference/grpc-api.md#registerkafkasource).

### `consolidation` — IR-to-archive consolidation

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable/disable consolidation planner |
| `policies` | array | `[]` | Ordered list of consolidation policies (waterfall). If empty, a default `time_window(1h)` policy is used. |
| `stale_buffering_mins` | int | `60` | Minutes before an `IR_ARCHIVE_BUFFERING` file is auto-promoted to `IR_ARCHIVE_CONSOLIDATION_PENDING`. Negative value disables. See [Stuck-File Promotion](../concepts/consolidation.md#stuck-file-promotion). |

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
