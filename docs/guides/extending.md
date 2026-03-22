# Extending Metalog

[← Back to docs](../README.md)

Metalog is designed so that enterprise deployments can add custom behavior
without forking the core repository. This guide explains the extension model,
walks through each extension point with real-world examples, and covers
practical concerns like monorepo integration, proto wire compatibility, and
keeping OSS and enterprise code in sync.

---

## Design Philosophy

Metalog deliberately excludes opinions about RPC frameworks, cloud SDKs, Kafka
clients, secret management, and deployment topology. These are infrastructure
decisions that vary across organizations. Instead, metalog exposes small Go
interfaces at each integration boundary and provides a global registry for
runtime discovery.

The result is a clean separation:

| Layer | Metalog (OSS) | Your Code |
|-------|--------------|-----------|
| Business logic | Ingestion batching, schema evolution, query engine, consolidation planning, retention, task queue, HA | — |
| Data model | `FileRecord`, lifecycle states, column registry | — |
| Transport | Reference gRPC handlers (`grpcserver/`) | Your RPC handlers (YARPC, Connect, Twirp, etc.) |
| Storage | S3/filesystem/HTTP | Your internal blob store |
| Kafka | confluent-kafka-go consumer | Your managed Kafka proxy |
| Config & secrets | Plain YAML, plain strings | Your secret manager integration |
| Auth | None | Your auth middleware |

This means you never fork metalog. Every customization is a Go package that
imports metalog, implements an interface, and registers itself.

---

## How Extensions Work

Every extension point follows three steps:

1. **Interface** — a small Go interface in the metalog package
2. **Registry** — a thread-safe global map with `Register*()` and `New*()`/`Create*()` functions
3. **Activation** — configuration selects a registered name at runtime

Extensions live in a separate Go module (or a package within your monorepo)
that imports metalog. A blank import in your binary's `main.go` ensures
`init()` fires and the extension is registered before the node starts:

```go
package main

import (
    _ "your.company/metalog-ext/storage/internalblob" // registers "ib" backend
    _ "your.company/metalog-ext/kafka/proxy"           // registers custom transformers
)
```

This is the same pattern used by `database/sql` drivers and `image` decoders
in the Go standard library.

---

## Extension Points

### 1. Storage Backends

**Package:** `storage` · **Interface:** `Backend`

The storage interface is the simplest extension point and a good starting place.
It provides four operations over bucket-scoped or flat-key object storage:

```go
type Backend interface {
    Get(ctx context.Context, bucket, key string) (io.ReadCloser, error)
    Put(ctx context.Context, bucket, key string, body io.Reader, size int64) error
    Delete(ctx context.Context, bucket, key string) error
    Exists(ctx context.Context, bucket, key string) (bool, error)
}
```

**Built-in:** `"s3"` (S3/MinIO/GCS), `"fs"` (local filesystem), `"http"` (read-only HTTP)

#### Example: HTTP blob store backend

Many organizations have an internal blob storage service with an HTTP API.
Rather than embedding a proprietary SDK, you can wrap it in ~100 lines:

```go
package internalblob

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "time"

    "github.com/y-scope/metalog/storage"
)

func init() {
    storage.RegisterType("ib", storage.BackendMeta{
        RequiresBucket: false, // flat key space — bucket param is ignored
        Factory: func(cfg map[string]string) (storage.Backend, error) {
            baseURL := cfg["baseUrl"]
            if baseURL == "" {
                return nil, fmt.Errorf("internalblob: baseUrl is required")
            }
            return &httpBackend{
                baseURL: baseURL,
                client:  &http.Client{Timeout: 60 * time.Second},
            }, nil
        },
    })
}

type httpBackend struct {
    baseURL string
    client  *http.Client
}

func (b *httpBackend) Get(ctx context.Context, _, key string) (io.ReadCloser, error) {
    u, _ := url.JoinPath(b.baseURL, key)
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
    if err != nil {
        return nil, err
    }
    resp, err := b.client.Do(req)
    if err != nil {
        return nil, err
    }
    if resp.StatusCode == http.StatusNotFound {
        resp.Body.Close()
        return nil, storage.ErrObjectNotFound // use metalog's sentinel
    }
    if resp.StatusCode != http.StatusOK {
        resp.Body.Close()
        return nil, fmt.Errorf("ib get: status %d", resp.StatusCode)
    }
    return resp.Body, nil
}

// Put, Delete, Exists follow the same pattern...
```

Key details:
- `RequiresBucket: false` tells metalog this backend uses flat keys. The
  `bucket` parameter is passed as `_` in your methods.
- Return `storage.ErrObjectNotFound` (not a custom error) so metalog's
  consolidation and retention code can distinguish "not found" from "failed."
- The factory receives `map[string]string` from the YAML config, so you
  parse your own settings (`baseUrl`, `timeout`, etc.).

**Activate** in `node.yaml`:

```yaml
storage:
  backends:
    archive-store:
      type: ib
      baseUrl: http://blobstore.internal:19617
```

---

### 2. Message Transformers (Kafka wire format)

**Package:** `kafka` · **Interface:** `MessageTransformer`

Converts raw Kafka message bytes into a protobuf `MetadataRecord`. Use this
when your data producers emit a wire format other than the standard protobuf
or JSON schemas — Avro, custom binary, Spark event payloads, etc.

```go
type MessageTransformer interface {
    Transform(payload []byte) (*pb.MetadataRecord, error)
}
```

**Built-in:** `""` / `"auto"` (auto-detect JSON/protobuf), `"proto"` (protobuf only)

#### Why this exists

In practice, ingestion pipelines evolve. You start with a standard proto
format, then a team ships a Spark job that emits JSON with nested metadata,
then another team has an Avro-encoded stream. Rather than forcing all producers
to converge on one format, metalog lets each table declare its own transformer.

The built-in auto-detect transformer handles the common case (JSON starts with
`{`, everything else is treated as protobuf). For anything more complex, you
register a named transformer:

```go
package kafka

import mkafka "github.com/y-scope/metalog/kafka"

func init() {
    mkafka.RegisterTransformer("spark_event", func() mkafka.MessageTransformer {
        return &sparkTransformer{}
    })
}

type sparkTransformer struct{}

func (t *sparkTransformer) Transform(payload []byte) (*pb.MetadataRecord, error) {
    // Parse Spark executor event JSON, extract IR path, timestamps,
    // build MetadataRecord with dimensions like executor_id, app_name
    // ...
}
```

**Activate** per-table via `_table_config`:

```json
{ "kafka": { "record_transformer": "spark_event" } }
```

#### Self-describing key-value format

For producers that can't emit full protobuf, metalog's built-in JSON
transformer supports a self-describing key-value format where the key encodes
the field type:

```json
{
  "self_describing_kv": [
    {"key": "dim/str128/hostname", "value": "web-42"},
    {"key": "dim/int/status_code", "value": "200"},
    {"key": "agg_int/GTE/latency_ms/p99", "value": "150"},
    {"key": "sketch/sbbf/trace_id", "value": "<base64-encoded-bloom-filter>"}
  ]
}
```

This means producers don't need to know the metastore schema. They emit
typed key-value pairs, and the transformer routes them to the correct proto
fields. New dimensions and aggregations are auto-discovered by schema
evolution — no metastore-side changes needed.

See [Write Transformers](write-transformers.md) for a full walkthrough.

---

### 3. Kafka Adapters (transport replacement)

**Package:** `node` · **Interface:** `KafkaAdapter`

Replaces the entire Kafka transport layer. Unlike the other extension points
which use a global registry, Kafka adapters are wired programmatically via a
`NodeOption` — because swapping the transport is a deployment-level decision,
not a per-table one.

```go
type KafkaAdapter interface {
    Start(ctx context.Context)  // blocks until ctx is done
    Stop()                      // pre-cancel cleanup
}

type KafkaAdapterFactory func(
    tableName, tableID string,
    tableCfg metastore.TableConfig,
    ingestSvc *ingestion.Service,
    log *zap.Logger,
) (KafkaAdapter, error)
```

#### Why this is a NodeOption, not a registry

Storage backends and transformers are per-table concerns — different tables can
use different backends or wire formats. But the Kafka transport is typically a
deployment-wide choice dictated by infrastructure (your managed Kafka service,
your consumer proxy, your operational tooling). Making it a `NodeOption` keeps
the decision in `main.go` where it belongs, rather than in per-table config
where it would be confusing.

#### Example: push-based Kafka proxy adapter

Organizations with managed Kafka infrastructure often have a push-based
consumer proxy rather than a pull-based consumer. The proxy delivers messages
via gRPC push, and the service returns commit/retry/stash decisions per
message. This inverts the typical consumer pattern.

The adapter bridges this by managing a gRPC server with a dynamic topic
handler registry:

```go
// adapter.go — implements node.KafkaAdapter
type proxyAdapter struct {
    server  *ProxyServer
    handler *TableHandler
    topic   string
}

func (a *proxyAdapter) Start(ctx context.Context) {
    // Register this table's handler with the shared proxy server.
    // The server routes incoming messages to handlers by topic.
    a.server.Register(a.topic, a.handler)
    <-ctx.Done() // block until the node shuts down this table
}

func (a *proxyAdapter) Stop() {
    // Deregister before context cancellation, so in-flight messages
    // for this topic are drained gracefully.
    a.server.Deregister(a.topic)
}
```

The factory checks whether Kafka is configured for the table and returns the
sentinel error if not:

```go
func NewProxyAdapterFactory(server *ProxyServer) node.KafkaAdapterFactory {
    return func(
        tableName, tableID string,
        tableCfg metastore.TableConfig,
        ingestSvc *ingestion.Service,
        log *zap.Logger,
    ) (node.KafkaAdapter, error) {
        if !tableCfg.Kafka.Enabled || tableCfg.Kafka.Topic == "" {
            return nil, node.ErrKafkaNotConfigured
        }
        transformer := NewTransformer(tableCfg.Kafka.RecordTransformer)
        handler := NewTableHandler(tableName, transformer, ingestSvc)
        return &proxyAdapter{
            server:  server,
            handler: handler,
            topic:   tableCfg.Kafka.Topic,
        }, nil
    }
}
```

Key design details from real-world implementation:

- **Dynamic registration.** The proxy server is long-lived (started once at
  service boot), but tables come and go as coordinator units claim and release
  them. The `Register`/`Deregister` pattern lets the adapter lifecycle track
  the coordinator lifecycle without restarting the server.

- **Flush-before-commit.** The `TableHandler` blocks on the ingestion
  pipeline's flush callback before returning success to the proxy. This
  ensures at-least-once delivery — the proxy doesn't commit the offset until
  the record is durably written to the database.

- **Concurrent message handling.** The proxy delivers batches of messages.
  Handler goroutines process them concurrently, with actions collected via a
  channel and sent back to the proxy in order.

**Wire in your binary:**

```go
func main() {
    proxySrv, _ := proxy.NewServer(9091, log)
    n, _ := node.NewNode(cfg, log,
        node.WithKafkaAdapterFactory(proxy.NewProxyAdapterFactory(proxySrv)),
    )
    proxySrv.Start()
    n.Start()
}
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
func init() {
    consolidation.RegisterPolicyType("tenant_isolate", func(config json.RawMessage) (consolidation.Policy, error) {
        var cfg struct{ TenantDim string `json:"tenant_dim"` }
        if len(config) > 0 {
            if err := json.Unmarshal(config, &cfg); err != nil {
                return nil, err
            }
        }
        return &tenantIsolatePolicy{dim: cfg.TenantDim}, nil
    })
}
```

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

### 5. Telemetry Exporters (metrics backend)

**Package:** `telemetry` · **Interface:** `ExporterFactory`

Plugs in a custom OpenTelemetry metrics exporter. The built-in Prometheus
exporter serves metrics on the health port's `/metrics` endpoint. Enterprise
deployments can register additional exporters (Datadog, M3, Cortex, etc.)
to send metrics to their internal observability stack.

```go
type ExporterFactory func(cfg map[string]string) (sdkmetric.Reader, error)
```

**Built-in:** `"prometheus"` (default — serves `/metrics` on health port)

**Register:**

```go
package metrics

import (
    "github.com/y-scope/metalog/telemetry"
    sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func init() {
    telemetry.RegisterExporter("m3", func(cfg map[string]string) (sdkmetric.Reader, error) {
        endpoint := cfg["endpoint"]
        // Create your M3/Datadog/custom exporter and return it as a Reader
        return newM3Reader(endpoint)
    })
}
```

**Activate** in `node.yaml`:

```yaml
telemetry:
  enabled: true
  exporter: m3
  options:
    endpoint: "m3-collector.internal:9000"
```

The `options` map is passed directly to your factory — parse whatever
keys your exporter needs. The built-in `"prometheus"` exporter ignores
options (it uses the health port).

---

### 6. Record Transformers (semantic enrichment)

**Package:** `coordinator/ingestion` · **Interface:** `RecordTransformer`

> **Note:** The `RecordTransformer` registry exists but is not yet wired into
> the production ingestion pipeline. The interface is available for internal
> use and will be connected in a future release.

Intended to transform structured key-value data into `FileRecord` dimension
and aggregation fields after the `MessageTransformer` stage.

```go
type RecordTransformer interface {
    Transform(rec *metastore.FileRecord, data map[string]string) error
}
```

**Built-in:** `""` / `"default"` (maps all keys as dimensions), `"json"` (parses and flattens JSON)

---

## Practical Concerns

### RPC framework integration

Metalog's reference `grpcserver/` package is a complete gRPC implementation,
but many organizations use different RPC frameworks. The key insight is that
metalog's handlers are thin wrappers — the real logic lives in the domain
packages:

| Domain Package | What It Does | Your Handler Calls |
|---------------|-------------|-------------------|
| `coordinator.TableRegistration` | Table CRUD | `RegisterTable()`, `SetColumnAlias()`, `InvalidateColumn()` |
| `ingestion.Service` | Record ingestion with backpressure | `Ingest()`, `IngestWithCallbackWait()` |
| `metastore.MetadataReader` | Catalog queries | `ListTables()`, `ListDimensions()`, `ListAggs()`, `ListSketches()` |
| `query.SplitQueryEngine` | Split queries with streaming | `StreamSplitsAsync()` |
| `query.ResolveSplit()` | Column resolution | Converts raw DB rows to transport-agnostic `ResolvedSplit` |

Your RPC handlers follow a simple pattern: validate request → call domain
package → map domain result to your proto response → map domain error to
your framework's status codes. Each handler is typically 30-60 lines.

#### Proto wire compatibility

If your RPC framework uses a different proto generator (gogo-proto, protoc-gen-go
v1, etc.), you may need a conversion layer between your generated types and
metalog's standard protobuf types. The critical rule: **proto wire format is
determined by field numbers and types, not by the code generator.** As long as
your `.proto` files use the same field numbers and types as metalog's, the wire
format is identical. This means:

- Clients built against metalog's standard stubs can call your service
- Your service can interoperate with other metalog consumers (e.g.
  presto-clp-connector) without translation

If you use a different proto code generator, you'll need a thin conversion
package that maps between your generated types and metalog's `FileRecord`.
Place this in a shared package (not inside a transport handler) so both your
RPC and Kafka paths can reuse it.

### Proto conversion layer architecture

When both RPC and Kafka ingestion paths exist, they both need proto-to-domain
conversion. The conversion should be a shared leaf package:

```
rpc/ingestion.go   ──imports──►  protoconv/  ──imports──►  metalog/metastore
rpc/query.go       ──imports──►  metalog/query
kafka/handler.go   ──imports──►  protoconv/  ──imports──►  metalog/metastore
```

If the conversion lived inside the RPC handler package, the Kafka handler would
need to import the RPC layer just to convert records — coupling two unrelated
transports. A shared `protoconv` package keeps them independent.

### Monorepo and Bazel integration

If your organization uses a Go monorepo with Bazel, metalog integrates as a
`go_repository` pinned to a specific commit:

```python
go_repository(
    name = "com_github_y_scope_metalog",
    importpath = "github.com/y-scope/metalog",
    commit = "32d24fb...",
)
```

**Update workflow:**

1. Develop and test changes in your metalog fork/branch
2. Run `go test ./...` upstream
3. Push to GitHub
4. Update the commit hash in your `.bzl` file
5. Run `bazel test` on your service targets
6. If packages were added/removed, run `gazelle` on affected directories

**Design rule:** If a change is useful to any metalog user, it goes upstream.
If it requires your infrastructure, it stays in your service.

### Configuration and secrets

Metalog's `config.NodeConfig` is a plain Go struct parsed from YAML. Extend
it by embedding:

```go
type myConfig struct {
    config.NodeConfig `yaml:",inline"`
    ProxyPort         int `yaml:"proxy_port"`
}
```

Database passwords, API keys, and other secrets are intentionally plain strings
in the config struct. How they get populated is your concern — read from a
secret manager, inject via environment variables, or resolve from a mounted
secrets file at startup. Metalog never touches the filesystem for secrets.

### Deployment topology

A single metalog binary can run as coordinator, worker, or both. Most
production deployments use two pools from the same binary:

| Pool | Role |
|------|------|
| **Coordinator pool** | Ingestion, schema evolution, task scheduling, retention scanning, Kafka consumption |
| **Worker pool** | CLP compression and consolidation tasks |

The pool is selected at runtime via a config file or environment variable.
This keeps the binary identical and simplifies rollouts.

---

## What Metalog Provides That Makes Extending Easier

1. **Transport-agnostic domain types.** `FileRecord`, `ResolvedSplit`,
   `IngestionResult` are plain Go structs, not proto messages. Your transport
   layer maps to/from these without coupling to metalog's proto generator.

2. **Typed error contracts.** `IngestionResult.Err` carries sentinel errors
   (`ErrChannelFull`, `context.DeadlineExceeded`) and typed errors
   (`ValidationError`). Your handler maps these to framework-specific status
   codes (gRPC codes, HTTP status, YARPC errors) with a simple `switch`.

3. **`ResolveSplit()` for column resolution.** Raw database rows use physical
   column names (`dim_f01`, `agg_f03`). `ResolveSplit()` resolves these to
   logical names using the column registry, returning a clean struct. Both
   metalog's reference gRPC handler and your custom handler call this — ~20
   lines each instead of ~100 duplicated lines of column mapping.

4. **`ErrKafkaNotConfigured` sentinel.** Your adapter factory returns this
   when a table doesn't use Kafka. The caller detects it and silently skips
   Kafka setup. No error logs, no special handling needed.

5. **`BackendMeta.RequiresBucket`.** Tells metalog whether your storage uses
   bucket-scoped addressing (like S3) or flat keys (like an HTTP blob store).
   Archive paths are constructed accordingly without backend-specific logic.

6. **Per-table config via `_table_config` JSON.** Transformer names, policy
   configs, and Kafka settings are per-table, not global. Tables with
   different producers or different consolidation needs coexist in the same
   metastore without code changes.

---

## Summary

| Extension Point | Interface | Registration | Config |
|----------------|-----------|--------------|--------|
| Storage Backends | `storage.Backend` | `storage.RegisterType()` | `node.yaml` `storage.backends` |
| Message Transformers | `kafka.MessageTransformer` | `kafka.RegisterTransformer()` | `_table_config` `kafka.record_transformer` |
| Kafka Adapters | `node.KafkaAdapter` | `node.WithKafkaAdapterFactory()` | Programmatic (`NodeOption`) |
| Consolidation Policies | `consolidation.Policy` | `consolidation.RegisterPolicyType()` | `_table_config` `consolidation.policies` |
| Telemetry Exporters | `telemetry.ExporterFactory` | `telemetry.RegisterExporter()` | `node.yaml` `telemetry.exporter` |
| Record Transformers | `ingestion.RecordTransformer` | `ingestion.RegisterRecordTransformer()` | Not yet wired |

---

## See Also

- [Write Transformers](write-transformers.md) — Transformer implementation walkthrough
- [Configure Tables](configure-tables.md) — Per-table config schema and activation
- [Configuration Reference](../reference/configuration.md) — `node.yaml` reference
- [Consolidation](../concepts/consolidation.md) — Policy evaluation and task distribution
- [Architecture Overview](../concepts/overview.md) — System components and data flow
