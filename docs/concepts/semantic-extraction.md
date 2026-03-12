# Semantic Extraction

[← Back to docs](../README.md)

CLP's semantic extraction pipeline: how structure is automatically discovered in log data, and how LLM-powered training produces schema files for semantic variable labeling.

---

## Overview

CLP provides semantic capabilities for any append-only, timestamped data: application logs, audit trails, event streams, agent traces, and more. Unlike systems that treat this data as opaque text, CLP automatically extracts semantic structure. This enables better compression, faster queries, and richer analytics.

**Key semantic capabilities:**

1. **Automatic structure extraction** — CLP infers format strings, extracts variables, and derives schemas from log data without requiring upfront definitions
2. **Semantic variable labeling** — Variables are labeled with their semantic meaning (e.g., `username`, `endpoint`, `duration_ms`) rather than generic types
3. **Schema organization** — Logs are organized by semantic structure using Merged Parse Trees (MPT) and Encoded Record Tables (ERTs)
4. **Semantic queries** — Query logs by meaning, not just exact text matches (e.g., "find all logs where `duration_ms > 100`")
5. **Cross-log analytics** — Aggregate and analyze logs using semantic fields across different log sources

> **Note:** Some semantic capabilities are currently implemented (format string inference, variable extraction, schema organization), while others are in development (LLM-powered semantic variable labeling via periodic training). This document describes both.

---

## log-surgeon

[log-surgeon](https://github.com/y-scope/log-surgeon) is a high-performance C++ parsing engine integrated into CLP for intelligent variable extraction and format string inference.

Unlike regex-based parsers, log-surgeon uses a **custom tagged Deterministic Finite Automaton (DFA) parser** with capabilities that regex libraries (such as RE2) cannot match:

**1. Single-pass variable extraction and format string inference:**

Regex parsers require multiple passes or complex patterns to extract variables and infer format strings. log-surgeon does both in a single pass.

**2. Variable patterns instead of full log type patterns:**

Regex requires writing a complete pattern for each unique log type. log-surgeon only requires specifying **variable patterns** (such as what an IP address or username looks like), and CLP auto-infers the final log template after extracting and labeling variables:

```
Regex approach:
  - Must write separate pattern for each log type:
    Pattern 1: r"User (\w+) requested (\S+) in (\d+)ms"
    Pattern 2: r"Requested (\S+) by user (\w+) in (\d+)ms"
  - Each format variation requires a new pattern
  - Not scalable: thousands of log types = thousands of patterns

log-surgeon approach:
  - Only specify variable patterns (once):
    - Username pattern: alphanumeric
    - Endpoint pattern: path-like
    - Duration pattern: numeric + "ms"
  - CLP auto-infers log templates
  - Scalable: variable patterns defined once, templates inferred automatically
```

**3. Semantic labeling via schema files:**

Variables can be extended with semantic labels via schema files, making them meaningful across format variations:

```
Regex: Static pattern, format-dependent
  r"executor-(\d+)" produces var_1

log-surgeon: Extensible, format-independent
  Variable pattern: numeric ID (works for "executor-7", "Executor ID: 7", etc.)
  Semantic label: executor_id (via schema file)
```

**Integration with CLP:**

- **Edge processing**: log-surgeon is integrated into CLP binaries used by log-collector for real-time parsing during CLP-IR file creation
- **Consolidation workers**: log-surgeon is used in consolidation workers when applying semantic labels from schema files
- **Python bindings**: [log-surgeon-ffi-py](https://github.com/y-scope/log-surgeon-ffi-py) provides Python FFI bindings for training and analysis workflows

---

## Architecture and Processing Flow

Semantic processing occurs at multiple stages in the CLP pipeline:

**Stage 1 — Edge (log-collector):** Format string inference, basic variable extraction (generic types: dictionary vs numeric). Output: CLP-IR files with row-based compression.

**Stage 2 — Periodic Training (separate service):** LLM-powered iterative process using `log-surgeon-ffi-py` to analyze production logs and produce schema files with semantic variable labels. Not in the ingestion path. Frequency: periodic (daily or weekly).

**Stage 3 — Consolidation Workers:** Load schema files, apply semantic labels during CLP-IR to CLP-Archive consolidation, extract variable keys and sync to metastore. See [Consolidation](consolidation.md).

**Stage 4 — Metastore:** Variable keys (and optionally values) stored in the `tags` JSON column for fast metadata lookup and file pruning. See [Metadata Tables](../reference/metadata-tables.md).

**Stage 5 — Query Layer:** Generated configs (from schema files) expose variables to Presto connectors, query APIs, and analytics tools as queryable semantic fields.

```
[Stage 1: Edge]                      [Stage 2: Training — periodic]
Logs -> log-surgeon -> IR files       Logs -> LLM -> Schema files
         |                                               |
         |                  [Stage 3: Workers]           |
         +----------------> IR + Schema -> Archive <-----+
                                        |
                                        | (variable sync)
                                        v
                            [Stage 4: Metastore]
                            tags JSON column
                                        |
                                        v
                            [Stage 5: Query]
                            Presto, APIs, tools
```

**Key points:**
- Edge processing (Stage 1) produces CLP-IR files during ingestion
- Training (Stage 2) is separate and periodic, producing schema files
- Consolidation (Stage 3) is where semantic labels are applied asynchronously
- Workers poll schema files periodically for the latest version per table/service

---

## Current Semantic Extraction

These capabilities work automatically during ingestion and form the foundation for LLM-powered semantic labeling.

### Format String Inference

CLP automatically infers repetitive patterns in log messages by separating static text from dynamic variables:

```
Raw log messages:
  "User alice logged in from 192.168.1.1"
  "User bob logged in from 192.168.1.2"
  "User charlie logged in from 192.168.1.3"

CLP extracts:
  Format String                    Var 1     Var 2
  "User {} logged in from {}"     alice     192.168.1.1
  "User {} logged in from {}"     bob       192.168.1.2
  "User {} logged in from {}"     charlie   192.168.1.3
```

**Benefits:**
- **Compression**: Format strings stored once in a dictionary and referenced many times (2x better than Gzip)
- **Search efficiency**: Search format strings separately from variables
- **Automatic**: No manual pattern definitions required

### Variable Extraction and Labeling

Variables are categorized into two generic types:

| Variable Type | Examples | Storage Strategy |
|---------------|----------|------------------|
| **Dictionary variables** | usernames, service names, IP addresses | Store in dictionary, reference by index |
| **Numeric variables** | request IDs, durations, counts | Binary format (compact for high-entropy data) |

**Future capability (not yet open-sourced):** Bloom filters for high-cardinality values enable probabilistic matching — determining whether a file can contain a specific value before searching.

### Schema Extraction (MPT and ERTs)

For semi-structured log data (JSON, key-value pairs), CLP automatically extracts schemas and organizes records by their semantic structure during consolidation (Stage 3).

**Merged Parse Tree (MPT)** represents all unique schema structures in a dataset, storing each schema only once. The MPT knows **all keys, hierarchy, nesting, and types** of the data without reading actual content. This enables:
- Query optimization: know which ERTs might contain matching records before scanning
- Columnar access: access only the variables needed for a query
- Upper-layer exposure: LLMs or analytics tools can understand data shape without reading content

**Encoded Record Tables (ERTs)** group records sharing the same schema into nested tables where each table is perfectly structured, with all records having identical keys and value types. Values are organized by column, grouping semantically similar data for maximum compression.

```
Example: Logs with varying schemas

Log 1: {"user": "alice", "action": "login", "timestamp": 1234567890}
Log 2: {"user": "bob", "action": "logout", "timestamp": 1234567900}
Log 3: {"request_id": "req-123", "status": 200, "latency_ms": 42}

MPT stores:
  Schema 1: {user, action, timestamp}
  Schema 2: {request_id, status, latency_ms}

ERTs organize:
  ERT 1 (Schema 1): [Log 1, Log 2]
  ERT 2 (Schema 2): [Log 3]
```

**Log Types** are automatically inferred format strings/templates. Instead of regex scanning every byte, CLP can match against log type templates first (fast dictionary lookup), only decompress and scan variables when the template matches, and skip entire log types that cannot match the pattern.

---

## LLM-Powered Schema Generation

The current heuristic (dictionary vs numeric) can be improved. Variables carry **domain-specific meaning** (e.g., `application-id`, `hostname`, `duration_ms`). LLM-powered schema generation uses periodic training to produce schema files that define semantic variable labels.

```
Current (generic types):
  "User alice requested /api/users in 42ms"
    dict_var: "alice"          (generic text)
    dict_var: "/api/users"     (generic text)
    num_var:  42               (generic number)

With semantic labels:
  "User alice requested /api/users in 42ms"
    username:     "alice"      (labeled: username)
    endpoint:     "/api/users" (labeled: endpoint)
    duration_ms:  42           (labeled: duration_ms)
```

### The Process

Schema generation follows an iterative refinement methodology:

1. **Initialize**: Begin with an empty template script
2. **Timestamp identification**: Read ~10 lines to determine timestamp format
3. **Pattern discovery**: Use `log-surgeon-ffi-py` to analyze logs and extract preliminary format strings
4. **LLM variable extraction**: LLM analyzes log types, recognizes recurring patterns, extracts semantic variables using `PATTERN` building blocks
5. **Precision refinement**: Show multiple similar log messages per type via `get_log_type_with_sample()` for precise extraction
6. **Measurement**: Quantify reduction in unique log types after each iteration
7. **Export schema**: Generate the final schema file for consolidation workers

**Success metric**: Unique log types reduced by over 99% (e.g., Spark 1.6: 63,500+ -> 37 log types).

### Best Practices

**1. Pattern ordering is critical:** Define more specific patterns before more general ones. Pattern matching is order-dependent — if a general pattern matches first, accurate variable extraction is prevented. Spark-specific IDs before generic numbers, URLs before file paths.

**2. Use `PATTERN` building blocks:** Prefer pre-tested components (`PATTERN.IPV4`, `PATTERN.PORT`, `PATTERN.FLOAT`, `PATTERN.JAVA_FULLY_QUALIFIED_CLASS_NAME`) over custom regex. Run `explore_patterns.py` to see all available components.

**3. Include surrounding text for context:** `"lost executor: (?<lost_executor_id>\d+)"` is far more meaningful than `(?<id>\d+)`. Context-aware extraction produces semantically rich variables.

**4. Escape special characters:** The hyphen (`-`) must be escaped as `\-` in all patterns, not just within character ranges.

**5. Leverage domain knowledge:** Platform-specific knowledge enables dramatic reductions. Spark embeds timestamps in IDs (`app-20160504043056-0000`) — extracting this as a variable consolidated thousands of patterns into one.

| Platform | Recognized Patterns | Semantic Variables |
|----------|---------------------|-------------------|
| **Apache Spark** | Executor lifecycle, job stages, shuffle operations | `executor_id`, `spark_app_id`, `stage_id`, `task_id` |
| **Kubernetes** | Pod lifecycle, container events, node operations | `pod_name`, `namespace`, `container_id`, `node_name` |
| **Hadoop/HDFS** | Block operations, datanode events | `block_id`, `datanode_id`, `file_path` |
| **Cassandra** | Compaction, repair, read/write operations | `keyspace`, `table`, `compaction_type` |

### Real-World Example: Spark Log Parser

This example is based on actual schema generation on Spark 1.6 logs that reduced 63,500+ unique messages to 37 log types — a **99.94% reduction**.

#### Starting Point

The process begins with an empty template script (`template.py`) that provides parser structure, LLM instructions, and pattern examples. The template gives the LLM:
- Instructions to only read ~10 lines for timestamp identification
- Guidance to use `log-surgeon-ffi-py` for all subsequent analysis
- `PATTERN` building block references
- Best practices for context-aware labeling and pattern ordering

<details>
<summary><strong>View template.py (starting point)</strong></summary>

```python
from log_surgeon import Parser, Query, PATTERN

parser = Parser(delimiters=rf" \t\r\n:,!;%@/()[].")

# Step 1 - Timestamp pattern
# parser.add_timestamp("hdfs", rf"\d{{4}}\-\d{{2}}\-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}},\d{{3}}")

# Step 2 - Verbosity level
# parser.add_var("LEVEL", rf"(?<level>(INFO)|(WARN)|(ERROR))")

# Step 3 - Java stack traces
# parser.add_var("SYSTEM_EXCEPTION",
#     rf"(?<system_exception_type>({PATTERN.JAVA_PACKAGE_SEGMENT})+Exception): "
#     rf"(?<system_exception_msg>{PATTERN.LOG_LINE})")
# parser.add_var("SYSTEM_STACK_TRACE",
#     rf"(\s{{1,4}}at (?<system_stack>{PATTERN.JAVA_STACK_LOCATION})")

# Step 4 - Custom variable patterns
# parser.add_var("SYSTEM_IP_PORT", rf"(?<ip>{PATTERN.IPV4}):(?<port>{PATTERN.PORT})")

parser.compile()

# For JSON logs, use JsonParser:
# from log_surgeon import JsonParser
# json_parser = JsonParser(parser)
# Query(json_parser).from_(file_obj).get_log_type_with_sample()
```

</details>

#### Iteration Progression

| Iteration | Patterns Added | Log Types | Reduction | Key Insight |
|-----------|----------------|-----------|-----------|-------------|
| **V1** | Timestamp, level, component | 63,565 | - | Baseline establishment |
| **V2** | IP:PORT, memory, cores | 63,153 | 0.6% | Minimal initial impact |
| **V3** | Spark IDs (app_id, worker_id) | 588 | **99.1%** | Breakthrough: ID extraction prevents pattern explosion |
| **V4** | Context-aware executor patterns | 55 | 99.9% | Contextual patterns improve specificity |
| **V5** | URLs (prior to file paths) | 37 | **99.94%** | Critical importance of ordering |

**Iteration 3 — the breakthrough:** Spark embeds timestamps in IDs (e.g., `app-20160504043056-0000`). Without extraction, each unique timestamp creates a new log type. Extracting these IDs collapsed thousands of patterns into one:

```python
# Most specific patterns first
parser.add_var("APP_EXECUTOR_ID", rf"(?<app_executor_id>app\-\d{{14}}\-\d{{4}}/\d+)")
parser.add_var("APP_ID", rf"(?<app_id>app\-\d{{14}}\-\d{{4}})")
parser.add_var("WORKER_ID", rf"(?<worker_id>worker\-\d{{14}}\-{PATTERN.IPV4}\-{PATTERN.PORT})")
parser.add_var("DRIVER_ID", rf"(?<driver_id>driver\-\d{{14}}\-\d{{4}})")
```

**Iteration 4 — context-aware patterns:**

```python
parser.add_var("LOST_EXECUTOR", rf"lost executor: (?<lost_executor_id>\d+)")
parser.add_var("KILL_EXECUTORS", rf"kill executors: (?<executor_ids_to_kill>\d+)")
```

**Iteration 5 — URL ordering:**

```python
# URLs must be defined before file paths!
parser.add_var("SPARK_URL", rf"(?<spark_url>spark://[a-zA-Z0-9\-\.]+:{PATTERN.PORT})")
parser.add_var("HTTP_URL", rf"(?<http_url>http://[a-zA-Z0-9\-\.]+:{PATTERN.PORT})")
parser.add_var("FILE_PATH", rf"(?<file_path>/[{PATTERN.LINUX_FILE_NAME_CHARSET}/]+)")
```

<details>
<summary><strong>View final generated parser (spark_log_parser.py)</strong></summary>

```python
from log_surgeon import Parser, Query, PATTERN

def create_spark_parser():
    parser = Parser(delimiters=rf" \t\r\n:,!;%@/\(\)\[\]\.")

    # Timestamp (YY/MM/DD HH:MM:SS)
    parser.add_timestamp("spark", rf"\d{{2}}/\d{{2}}/\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}")

    # Log level and Java component
    parser.add_var("LEVEL", rf"(?<level>(INFO)|(WARN)|(ERROR)|(DEBUG))")
    parser.add_var("COMPONENT", rf"(?<component>{PATTERN.JAVA_FULLY_QUALIFIED_CLASS_NAME})")

    # Spark-specific IDs (most specific first)
    parser.add_var("APP_EXECUTOR_ID", rf"(?<app_executor_id>app\-\d{{14}}\-\d{{4}}/\d+)")
    parser.add_var("APP_ID", rf"(?<app_id>app\-\d{{14}}\-\d{{4}})")
    parser.add_var("WORKER_ID", rf"(?<worker_id>worker\-\d{{14}}\-{PATTERN.IPV4}\-{PATTERN.PORT})")
    parser.add_var("DRIVER_ID", rf"(?<driver_id>driver\-\d{{14}}\-\d{{4}})")

    # Context-aware executor patterns (before generic numbers)
    parser.add_var("LOST_EXECUTOR", rf"lost executor: (?<lost_executor_id>\d+)")
    parser.add_var("KILL_EXECUTORS", rf"kill executors: (?<executor_ids_to_kill>\d+)")
    parser.add_var("NONEXISTENT_EXECUTORS", rf"non\-existent executors: (?<nonexistent_executor_ids>\d+)")

    # Network addresses
    parser.add_var("SPARK_HOST_PORT", rf"(?<spark_host_port>spark\-{PATTERN.INT}:{PATTERN.PORT})")
    parser.add_var("IP_PORT", rf"(?<ip_port>{PATTERN.IPV4}:{PATTERN.PORT})")

    # URLs (before FILE_PATH)
    parser.add_var("SPARK_URL", rf"(?<spark_url>spark://[a-zA-Z0-9\-\.]+:{PATTERN.PORT})")
    parser.add_var("HTTP_URL", rf"(?<http_url>http://[a-zA-Z0-9\-\.]+:{PATTERN.PORT})")
    parser.add_var("HTTPS_URL", rf"(?<https_url>https://[a-zA-Z0-9\-\.]+:{PATTERN.PORT})")

    # Resource metrics
    parser.add_var("MEMORY_SIZE", rf"(?<memory_size>{PATTERN.FLOAT}) MB")
    parser.add_var("MEMORY_SIZE_GB", rf"(?<memory_gb>{PATTERN.FLOAT}) GB")
    parser.add_var("CORE_COUNT", rf"(?<core_count>{PATTERN.INT}) cores")

    # Misc patterns
    parser.add_var("PORT_NUM", rf"port (?<port>{PATTERN.PORT})")
    parser.add_var("SERVICE_NAME", rf"'(?<service_name>[a-zA-Z0-9_\-]+)'")
    parser.add_var("VERSION", rf"(?<version>[0-9]+\.[0-9]+\.[0-9]+)")
    parser.add_var("BYTE_SIZE", rf"(?<byte_size>\d+) bytes")
    parser.add_var("SPARK_HOST", rf"(?<spark_host>spark\-{PATTERN.INT})")

    # File paths (last — most general)
    parser.add_var("FILE_PATH", rf"(?<file_path>/[{PATTERN.LINUX_FILE_NAME_CHARSET}/]+)")

    parser.compile()
    return parser
```

</details>

#### Example Output

```
Pattern: <timestamp> <level> <component>: Launching executor <app_executor_id> on worker <worker_id>
Sample:  16/05/04 04:30:56 INFO master.Master: Launching executor app-20160504043056-0000/0
         on worker worker-20160504042454-192.168.10.199-44855

Pattern: <timestamp> <level> <component>: lost executor: <lost_executor_id> on <worker_id>
Sample:  16/05/04 04:35:12 WARN master.Master: lost executor: 177
         on worker worker-20160504042454-192.168.10.135-41161
```

#### Exported Schema File

```yaml
variables:
  - name: app_id
    pattern: "app\-\d{{14}}\-\d{{4}}"
    type: string
    platform: apache_spark

  - name: worker_id
    pattern: "worker\-\d{{14}}\-{PATTERN.IPV4}\-{PATTERN.PORT}"
    type: string
    platform: apache_spark

  - name: lost_executor_id
    pattern: "lost executor: (?<lost_executor_id>\d+)"
    type: string
    platform: apache_spark

  - name: memory_size
    pattern: "{PATTERN.FLOAT} MB"
    type: float
    platform: universal
```

**Iterative vs comprehensive:** Both approaches achieve identical results. The iterative approach (shown above) is best for unknown/evolving log formats. The comprehensive approach (complete upfront analysis) is best for well-understood domains. References: [Iterative example](https://github.com/y-scope/log-surgeon-schema-generation/tree/main/experiment-nov-2/claude-attempt-2) - [Comprehensive example](https://github.com/y-scope/log-surgeon-schema-generation/tree/main/experiment-nov-2/claude-attempt-3)

### Schema Files and Variable Patterns

Schema files define variable patterns (not full log type patterns) that log-surgeon uses during parsing. log-surgeon extracts variables regardless of position in the log message, CLP auto-infers templates, and variables are labeled with semantic names from the schema file.

### Layered Schemas

Similar to Docker images, schemas can inherit from base layers. Teams start from a shared foundation and extend with their own patterns:

```
Application Team Schema
  FROM: platform-team/microservices-base:v2.1
  + order_id, customer_id, cart_total
        |
        | inherits
        v
Platform Team Base
  FROM: infra-team/k8s-base:v3.0
  + request_id, trace_id, span_id
        |
        | inherits
        v
Infrastructure Team Base
  + pod_name, namespace, container_id
  + timestamp, log_level
```

```yaml
# Base schema: infra-team/k8s-base:v3.0
variables:
  - name: pod_name
    platform: kubernetes

# Extended schema: platform-team/microservices-base:v2.1
from: infra-team/k8s-base:v3.0
variables:
  - name: request_id
    platform: distributed_tracing

# Application schema: app-team/order-service:v1.0
from: platform-team/microservices-base:v2.1
variables:
  - name: order_id
    platform: business_logic
```

**Benefits:** Consistency (`pod_name` means the same thing everywhere), traceability across layers, automatic onboarding for new teams, and base layer updates propagate like Docker image rebuilds.

### Schema Distribution and Application

**Distribution:** Consolidation workers poll a schema file repository periodically for the latest schema per table/service. Automated schemas are versioned and immediately available; user-contributed schemas undergo approval.

**Application in workers:**

1. **Schema loading**: Workers poll, load, and cache schemas per table/service (auto-reload on updates)
2. **Variable extraction**: CLP binaries apply schema patterns, extract variables, assign semantic labels, and place them in JSON output (root or nested, configurable via `JsonParser.on_conflict()`)
3. **Storage and sync**: Variable values stored in columnar CLP-Archive format. A configurable subset of high-value correlation fields (e.g., `pipeline_id`, `request_id`) is synced to the metastore `tags` column for file pruning

### Implementation Options

| Approach | Availability | Use Case |
|----------|-------------|----------|
| **Claude Messages API + Opus** | In development | Fully automated production training |
| **Claude Code / Cursor** | Available now | Interactive local development |
| **Agent SDK (TypeScript)** | Available now | Custom automation pipelines |

All approaches share the same foundation: template script, log-surgeon-ffi-py for processing, iterative refinement, and pattern extraction best practices.

---

## Query Capabilities

Semantic variables are exposed through generated configs, enabling type-safe, schema-aware queries:

```sql
-- Schema-defined field access
SELECT * FROM logs WHERE duration_ms > 1000

-- UDF access (schema-less, identical performance)
SELECT * FROM logs WHERE GET_CLP_INT('duration_ms') > 1000

-- JSON row projection
SELECT CLP_GET_JSON() FROM logs WHERE GET_CLP_STRING('username') = 'alice'
```

| Capability | Example | Advantage over Text Search |
|:---|:---|:---|
| **Semantic filtering** | `WHERE duration_ms > 1000` | Type-aware, not regex |
| **Aggregation** | `GROUP BY endpoint` | Works across log formats |
| **Cross-service correlation** | `JOIN ON request_id` | Consistent field names |
| **Hierarchical navigation** | Drill down: job -> stage -> executor | No manual correlation |

### Cross-Platform Correlation Example

CLP's semantic extraction enables multi-level log correlation that traditional text search cannot provide. When a job scheduler spawns sub-jobs across platforms (Spark, Presto, Kubernetes), semantic fields automatically enable cross-platform tracing:

```sql
-- Cross-platform correlation: find executor errors with related K8s events
SELECT s.executor_id, s.spark_app_id, s.error_message,
       k.pod_name, k.event_type
FROM logs s
JOIN logs k ON s.executor_id = k.container_id
WHERE s.job_id = 'job-20241201-001' AND s.log_level = 'ERROR'
  AND k.event_type IN ('pod_failed', 'container_oom')
ORDER BY s.timestamp
```

### File Selection Workflow

```sql
-- Step 1: Find files with specific variables and errors
WITH relevant_files AS (
  SELECT file_path FROM clp_files
  WHERE JSON_CONTAINS(tags->>'$.variable_values.application_id', '"app-20241201143052-0042"')
    AND JSON_CONTAINS(tags->>'$.variable_values.log_level', '"ERROR"')
    AND timestamp_start BETWEEN '2024-12-01 14:00:00' AND '2024-12-01 15:00:00'
)
-- Step 2: Query selected files
SELECT spark_app_id, stage_id, executor_id, error_message, timestamp
FROM logs
WHERE file_path IN (SELECT file_path FROM relevant_files)
  AND GET_CLP_STRING('log_level') = 'ERROR'
ORDER BY timestamp DESC LIMIT 100
```

---

## References

**Core Technologies:**
- [log-surgeon](https://github.com/y-scope/log-surgeon) — High-performance C++ parsing engine
- [log-surgeon-ffi-py](https://github.com/y-scope/log-surgeon-ffi-py) — Python FFI bindings for LLM training
  - [Documentation](https://y-scope.github.io/log-surgeon-ffi-py/beta/)
  - [Architecture](https://y-scope.github.io/log-surgeon-ffi-py/beta/architecture/)

**Real-World Implementation:**
- [log-surgeon-schema-generation](https://github.com/y-scope/log-surgeon-schema-generation) — Reference implementation
  - [Spark parser](https://github.com/y-scope/log-surgeon-schema-generation/tree/main/experiment-nov-2/claude-attempt-3/spark_log_parser.py)
  - [Template script](https://github.com/y-scope/log-surgeon-schema-generation/tree/main/template.py)

---

## See Also

- [Architecture Overview](overview.md) — System overview, data lifecycle
- [Consolidation](consolidation.md) — IR-Archive pipeline where schema files are applied
- [Metadata Tables](../reference/metadata-tables.md) — Database schema, tags column for variable keys
