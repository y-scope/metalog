# Kafka Source Assignment

## Problem

The current design couples Kafka consumption to `CoordinatorUnit` ownership — only the single node that owns a table via `_table_assignment` can consume Kafka for it. In multi-region deployments where Kafka topics exist on separate regional clusters (same topic name, different bootstrap servers) and the metastore DB is global, multiple nodes (one per region) need to consume independently for the same table.

## Design

### Separate table assignment from Kafka assignment

| Concept | Purpose | Owner | DB Table |
|---|---|---|---|
| **Table assignment** | Coordinator duties (planner, retention, schema, partition maintenance) | Single node globally | `_table_assignment` |
| **Kafka source assignment** | Kafka consumption and ingestion | One node per source (multiple per table) | `_kafka_assignment` |

### Kafka sources as a first-class entity

Kafka sources are decoupled from `TableConfig`. Each source is an independent row in `_kafka_source`, associated with a table but managed separately.

```sql
CREATE TABLE IF NOT EXISTS _kafka_source (
    table_name          VARCHAR(64) NOT NULL,
    source_name         VARCHAR(128) NOT NULL,
    topic               VARCHAR(255) NOT NULL,
    bootstrap_servers   VARCHAR(1024) NOT NULL,
    record_transformer  VARCHAR(64) DEFAULT 'proto',
    consumer_group_id   VARCHAR(255) NULL,
    required_env        VARCHAR(512) NULL,
    created_at          BIGINT NOT NULL,
    PRIMARY KEY (table_name, source_name),
    FOREIGN KEY (table_name) REFERENCES _table(table_name) ON DELETE CASCADE
);
```

#### `required_env` column

Format: `KEY=VALUE,KEY=VALUE` (comma-separated, AND semantics). A node can claim a source only if `os.Getenv(k) == v` for every pair. NULL means any node can claim.

Examples:
- `REGION=us-east,CLUSTER=prod` — node must have both env vars matching
- `REGION=eu-west` — single condition
- NULL — no restriction

Each source can have a different set of conditions. This supports deployments where different sources have different routing criteria.

### Runtime assignment

```sql
CREATE TABLE IF NOT EXISTS _kafka_assignment (
    table_name      VARCHAR(64) NOT NULL,
    source_name     VARCHAR(128) NOT NULL,
    node_id         VARCHAR(64) NULL,
    lease_expiry    BIGINT NULL,
    claimed_at      BIGINT NULL,
    PRIMARY KEY (table_name, source_name),
    INDEX idx_kafka_node (node_id),
    FOREIGN KEY (table_name, source_name)
        REFERENCES _kafka_source(table_name, source_name) ON DELETE CASCADE
);
```

Claiming follows the existing CAS pattern:
1. Node reads all unclaimed sources: `SELECT ... FROM _kafka_source ks LEFT JOIN _kafka_assignment ka USING (table_name, source_name) WHERE ka.node_id IS NULL`
2. Filters locally by `required_env` (parse string, check `os.Getenv` for each pair)
3. Claims matching sources: `UPDATE _kafka_assignment SET node_id = ? WHERE table_name = ? AND source_name = ? AND node_id IS NULL`

### Node lifecycle

```
Node startup / reconciliation tick
├── Table reconciliation (existing, unchanged)
│   └── CoordinatorUnit: planner, retention, schema, partition maintenance
│       └── NO LONGER starts Kafka adapters
│
└── Kafka source reconciliation (new, parallel)
    ├── SELECT all sources + assignments
    ├── Filter by required_env
    ├── Claim unclaimed matching sources
    └── Start KafkaIngestionUnit per claimed source
        ├── BatchingWriter (shared per table)
        ├── ColumnRegistry (shared per table)
        └── Kafka adapter (consumer → transformer → ingestion service)
```

### KafkaIngestionUnit

Lightweight component — just a Kafka adapter writing to BatchingWriter. No planner, no retention, no partition management (those stay in `CoordinatorUnit`).

```go
type KafkaIngestionUnit struct {
    tableName  string
    sourceName   string
    adapter    kafka.Adapter
    ingestSvc  *ingestion.Service
    log        *zap.Logger
    ctx        context.Context
    cancel     context.CancelFunc
    wg         sync.WaitGroup
}
```

## HA behavior

- **Node crash**: Source lease expires → another eligible node (same region) claims it
- **Region down**: Sources for that region stay unclaimed; other regions unaffected
- **Split brain**: Two nodes briefly consume same source; UPSERT is idempotent, Kafka consumer group rebalance resolves quickly
- **Independence**: Table coordinator (planner/retention) can be in any region. Kafka consumers are in their respective regions. Both write to the same global DB.

## Example: three-region deployment

```
_kafka_source:
  web_logs | us-east | web_logs | kafka-us:9092  | REGION=us-east
  web_logs | eu-west | web_logs | kafka-eu:9092  | REGION=eu-west
  web_logs | ap-south| web_logs | kafka-ap:9092  | REGION=ap-south

_table_assignment:
  web_logs | node-us-east-1   (coordinator owner — runs planner, retention)

_kafka_assignment:
  web_logs | us-east  | node-us-east-1   (consumes from kafka-us)
  web_logs | eu-west  | node-eu-west-1   (consumes from kafka-eu)
  web_logs | ap-south | node-ap-south-1  (consumes from kafka-ap)
```

All three nodes write to the same `web_logs` table in the global DB. Only `node-us-east-1` runs the coordinator (planner, retention, schema evolution).
