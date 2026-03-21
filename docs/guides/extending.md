# Extending Metalog

[← Back to docs](../README.md)

Metalog is designed so that enterprise deployments can add custom behavior
without forking the core repository. All extension points use the same pattern:
implement a Go interface, register it via `init()`, and activate it through
configuration.

---

## How Extensions Work

Every extension point follows the same three-step pattern:

1. **Interface** — a small Go interface in the metalog package (e.g. `MessageTransformer`, `Backend`)
2. **Registry** — a thread-safe global map of `name → factory` with `Register*()` and `Create*()`/`New*()` functions
3. **Activation** — configuration selects a registered name at runtime (per-table JSON config or `node.yaml`)

Extensions live in a separate Go module that imports metalog. A blank import
in your binary's `main.go` ensures `init()` runs and the extension is
registered before the node starts. Copy `cmd/metalog/main.go` as your
starting point and add blank imports for your extension packages:

```go
package main

import (
    _ "your.company/metalog-extensions/kafka"   // registers custom transformers
    _ "your.company/metalog-extensions/storage"  // registers custom storage backend
)

// ... rest of main.go copied from cmd/metalog/main.go
```

This is the same pattern used by `database/sql` drivers and `image` decoders
in the Go standard library.

---

## Extension Points

### 1. Message Transformers (Kafka wire format)

**Package:** `kafka` · **Interface:** `MessageTransformer`

Converts raw Kafka message bytes into a protobuf `MetadataRecord`. Use this
when your data sources produce a wire format other than the standard protobuf
or JSON schemas.

```go
type MessageTransformer interface {
    Transform(payload []byte) (*pb.MetadataRecord, error)
}
```

**Built-in:** `""` / `"auto"` (auto-detect JSON/protobuf), `"proto"` (protobuf only)

**Register:**

```go
package kafka

import mkafka "github.com/y-scope/metalog/kafka"

func init() {
    mkafka.RegisterTransformer("avro", func() mkafka.MessageTransformer {
        return &avroTransformer{}
    })
}
```

**Activate** per-table via `_table_config`:

```json
{ "kafka": { "record_transformer": "avro" } }
```

See [Write Transformers](write-transformers.md) for a full walkthrough
including field mapping, error handling, and dimension key formats.

---

### 2. Record Transformers (semantic enrichment)

**Package:** `coordinator/ingestion` · **Interface:** `RecordTransformer`

Transforms structured key-value data into `FileRecord` dimension and
aggregation fields. This layer runs after the `MessageTransformer` (or after
gRPC proto conversion) and handles semantic field mapping — deciding which
fields become dimensions, how to parse nested payloads, and what aggregations
to compute.

```go
type RecordTransformer interface {
    Transform(rec *metastore.FileRecord, data map[string]string) error
}
```

**Built-in:** `""` / `"default"` (maps all keys as dimensions), `"json"` (parses and flattens JSON)

**Register:**

```go
package ingestion

import "github.com/y-scope/metalog/coordinator/ingestion"

func init() {
    ingestion.RegisterRecordTransformer("spark", func() ingestion.RecordTransformer {
        return &sparkTransformer{}
    })
}
```

See [Write Transformers](write-transformers.md) for dimension/aggregation key
formats and examples.

---

### 3. Storage Backends

**Package:** `storage` · **Interface:** `Backend`

Provides object storage operations (get, put, delete, exists). Use this to
integrate with internal storage systems beyond the built-in S3, filesystem, and
HTTP backends.

```go
type Backend interface {
    Get(ctx context.Context, bucket, key string) (io.ReadCloser, error)
    Put(ctx context.Context, bucket, key string, body io.Reader, size int64) error
    Delete(ctx context.Context, bucket, key string) error
    Exists(ctx context.Context, bucket, key string) (bool, error)
}
```

**Built-in:** `"s3"` (S3/MinIO/GCS), `"fs"` (local filesystem), `"http"` (read-only HTTP)

**Register:**

```go
package storage

import mstorage "github.com/y-scope/metalog/storage"

func init() {
    mstorage.RegisterType("terrablob", mstorage.BackendMeta{
        RequiresBucket: true,
        Factory: func(cfg map[string]string) (mstorage.Backend, error) {
            return newTerrablobBackend(cfg["base_url"]), nil
        },
    })
}
```

The `BackendMeta.RequiresBucket` field tells the storage layer whether this
backend uses bucket-scoped addressing (like S3) or flat keys (like a
filesystem).

**Activate** in `node.yaml`:

```yaml
storage:
  backends:
    internal-blob:
      type: terrablob
      base_url: http://terrablob.internal:19617
```

---

### 4. Consolidation Policies

**Package:** `coordinator/consolidation` · **Interface:** `Policy`

Determines how files are grouped for consolidation (IR → archive). Each policy
receives candidate files and returns groups; the planner runs policies in a
waterfall (first policy gets all candidates, second gets unclaimed files, etc.).

```go
type Policy interface {
    SelectFiles(candidates []*metastore.FileRecord) []FileGroup
    RequiredDims() []string
    RequiredAggs() []AggRequirement
}
```

`RequiredDims` and `RequiredAggs` declare which columns must be populated on
the `FileRecord` for the policy to work — the planner ensures these columns are
fetched from the database.

**Built-in:** `"time_window"` (group by time range), `"spark_job"` (group by dimension value)

**Register:**

```go
package consolidation

import (
    "encoding/json"
    "github.com/y-scope/metalog/coordinator/consolidation"
)

func init() {
    consolidation.RegisterPolicyType("tenant_isolate", func(config json.RawMessage) (consolidation.Policy, error) {
        var cfg tenantConfig
        if len(config) > 0 {
            if err := json.Unmarshal(config, &cfg); err != nil {
                return nil, err
            }
        }
        return &tenantIsolatePolicy{dim: cfg.TenantDim}, nil
    })
}
```

Policy factories receive `json.RawMessage` directly — deserialize it into your
own config struct. A nil/empty config means "use defaults."

**Activate** per-table via `_table_config`:

```json
{
  "consolidation": {
    "policies": [
      { "type": "tenant_isolate", "config": { "tenant_dim": "org_id" } },
      { "type": "time_window", "config": { "window_size": "1h" } }
    ]
  }
}
```

Policies are evaluated in order (waterfall). See [Consolidation](../concepts/consolidation.md) for details.

---

### 5. Kafka Adapters (transport replacement)

**Package:** `node` · **Interface:** `KafkaAdapter`

Replaces the entire Kafka transport layer. Unlike the other extension points
which use a global registry, Kafka adapters are wired programmatically via a
`NodeOption` — because swapping the transport is a deployment-level decision,
not a per-table one.

```go
type KafkaAdapter interface {
    Start(ctx context.Context)  // blocks until ctx is done
    Stop()                      // pre-cancel cleanup (no-op for pull-based)
}

type KafkaAdapterFactory func(
    tableName, tableID string,
    tableCfg metastore.TableConfig,
    ingestSvc *ingestion.Service,
    log *zap.Logger,
) (KafkaAdapter, error)
```

The factory is called once per table. Return `(nil, node.ErrKafkaNotConfigured)`
if the table does not use this transport (e.g., Kafka not configured) — the
caller detects this sentinel and skips Kafka setup. `Start` blocks and consumes
messages until the context is cancelled. `Stop` is called before context
cancellation so push-based adapters can deregister cleanly.

**Built-in:** `kafka.NewDefaultAdapterFactory()` (confluent-kafka consumer)

**Wire in your binary:**

```go
package main

import (
    "github.com/y-scope/metalog/node"
    mykafka "your.company/metalog-extensions/kafka"
)

func main() {
    // ...
    n, err := node.NewNode(cfg, log,
        node.WithKafkaAdapterFactory(mykafka.NewKCPAdapterFactory()),
    )
}
```

---

## Summary

| Extension Point | Interface | Registration | Config |
|----------------|-----------|--------------|--------|
| Message Transformers | `kafka.MessageTransformer` | `kafka.RegisterTransformer()` | `_table_config` `kafka.record_transformer` |
| Record Transformers | `ingestion.RecordTransformer` | `ingestion.RegisterRecordTransformer()` | `_table_config` `kafka.record_transformer` |
| Storage Backends | `storage.Backend` | `storage.RegisterType()` | `node.yaml` `storage.backends` |
| Consolidation Policies | `consolidation.Policy` | `consolidation.RegisterPolicyType()` | `_table_config` `consolidation.policies` |
| Kafka Adapters | `node.KafkaAdapter` | `node.WithKafkaAdapterFactory()` | Programmatic (`NodeOption`) |

---

## See Also

- [Write Transformers](write-transformers.md) — Deep dive into transformer implementation
- [Configure Tables](configure-tables.md) — Per-table config schema and activation
- [Configuration Reference](../reference/configuration.md) — `node.yaml` reference
- [Consolidation](../concepts/consolidation.md) — Policy evaluation and task distribution
- [Architecture Overview](../concepts/overview.md) — System components and data flow
