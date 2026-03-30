# Extending Metalog

[← Back to docs](../README.md)

Metalog is designed so that enterprise deployments can add custom behavior
without forking the core repository. This guide explains the extension model,
walks through each extension point with real-world examples, and covers
practical concerns like monorepo integration, proto wire compatibility, and
keeping OSS and enterprise code in sync.

---

## Design Philosophy

Metalog deliberately excludes opinions about RPC frameworks, cloud SDKs,
secret management, and deployment topology. These are infrastructure
decisions that vary across organizations. Instead, metalog exposes small Go
interfaces at each integration boundary and provides a global registry for
runtime discovery.

The result is a clean separation:

| Layer | Metalog (OSS) | Your Code |
|-------|--------------|-----------|
| Business logic | Ingestion batching, schema evolution, query engine, consolidation planning, retention, task queue, HA | — |
| Data model | `FileRecord`, lifecycle states, column registry | — |
| Transport | Reference gRPC handlers (`grpcserver/`) | Your RPC handlers (Connect, Twirp, etc.) |
| Storage | S3/filesystem/HTTP | Your internal blob store |
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

### 2. Consolidation Policies

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

### 3. Telemetry Exporters (metrics backend)

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

### 4. Record Transformers (semantic enrichment)

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
| **Coordinator pool** | Ingestion, schema evolution, task scheduling, retention scanning |
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
   codes (gRPC codes, HTTP status, etc.) with a simple `switch`.

3. **`ResolveSplit()` for column resolution.** Raw database rows use physical
   column names (`dim_f01`, `agg_f03`). `ResolveSplit()` resolves these to
   logical names using the column registry, returning a clean struct. Both
   metalog's reference gRPC handler and your custom handler call this — ~20
   lines each instead of ~100 duplicated lines of column mapping.

4. **`BackendMeta.RequiresBucket`.** Tells metalog whether your storage uses
   bucket-scoped addressing (like S3) or flat keys (like an HTTP blob store).
   Archive paths are constructed accordingly without backend-specific logic.

5. **Per-table config via `_table_config` JSON.**
   Policy configs are per-table via `_table_config`.
   Tables with different producers or different consolidation needs coexist
   in the same metastore without code changes.

---

## Summary

| Extension Point | Interface | Registration | Config |
|----------------|-----------|--------------|--------|
| Storage Backends | `storage.Backend` | `storage.RegisterType()` | `node.yaml` `storage.backends` |
| Consolidation Policies | `consolidation.Policy` | `consolidation.RegisterPolicyType()` | `_table_config` `consolidation.policies` |
| Telemetry Exporters | `telemetry.ExporterFactory` | `telemetry.RegisterExporter()` | `node.yaml` `telemetry.exporter` |
| Record Transformers | `ingestion.RecordTransformer` | `ingestion.RegisterRecordTransformer()` | Not yet wired |

---

## See Also

- [Configure Tables](configure-tables.md) — Per-table config schema and activation
- [Configuration Reference](../reference/configuration.md) — `node.yaml` reference
- [Consolidation](../concepts/consolidation.md) — Policy evaluation and task distribution
- [Architecture Overview](../concepts/overview.md) — System components and data flow
