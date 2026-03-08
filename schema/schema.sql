-- ============================================================================
-- CLP Metadata Table Template
-- ============================================================================
--
-- Template table cloned by TableProvisioner for each managed table.
-- See: ../../../../docs/concepts/metadata-schema.md for schema design.
-- See: ../../../../docs/operations/performance-tuning.md for JDBC tuning and performance gotchas.
--
-- ============================================================================
-- DESIGN OVERVIEW
-- ============================================================================
--
-- Partitioning:
--   - RANGE partition by min_timestamp (daily, UTC midnight)
--   - Enables partition pruning for time-range queries
--   - Coordinator manages lifecycle (creates ahead, merges old sparse partitions)
--
-- Primary Key:
--   - (min_timestamp, id) - partition key first, then AUTO_INCREMENT
--   - Preserves write locality within partitions
--
-- Storage Columns:
--   - Bucket: VARCHAR(63) ASCII - matches S3 and GCS naming rules
--   - Path: VARCHAR(1024) - for projection only; use hash index for lookups
--
-- Dimensions:
--   - Opaque placeholder names (dim_f01, dim_f02, ...) mapped via _dim_registry
--   - ASCII charset by default (faster index scans); str_utf8 for Unicode
--
-- ============================================================================
-- KNOWN LIMITATIONS
-- ============================================================================
--
-- 1. IR Path Uniqueness is Per-Day, NOT Global
--    MySQL requires partition key in UNIQUE indexes, so:
--      UNIQUE(hash(ir_path), min_timestamp)
--    Same path with different min_timestamp creates duplicates.
--    Producer contract: min_timestamp is set once at file creation (first event's timestamp)
--    and remains immutable. All Kafka messages for the same IR path must use the same
--    min_timestamp to ensure UPSERT deduplication works correctly.
--
-- 2. Path Columns Are Not Directly Indexed
--    Path columns (clp_ir_path, clp_archive_path) are for projection only.
--    VIRTUAL columns (clp_ir_path_hash, clp_archive_path_hash) provide hash-based lookups.
--    Always use: WHERE clp_ir_path_hash = UNHEX(MD5(?))
--    Include min_timestamp for partition pruning.
--
-- 3. Object Storage Overwrites
--    S3 PUT overwrites existing keys silently.
--    Mitigation: Include timestamp or UUID in path generation.
--
-- 4. Column Names Are Case-Insensitive
--    MySQL/MariaDB column names are case-insensitive.
--    Convention: All names must be lowercase snake_case.
--    Physical columns use opaque placeholders (dim_f01, agg_f01); logical
--    field names are stored in _dim_registry/_agg_registry.
--
-- 5. MD5 Hash for Path Indexing via VIRTUAL Columns
--    Direct indexing of VARCHAR(1024) with utf8mb4 would require up to 4096 bytes per key
--    (1024 chars × 4 bytes/char), exceeding MySQL's 3072-byte index key limit.
--    VIRTUAL columns with MD5 hash (16 bytes) solve this while providing O(1) lookups.
--    VIRTUAL columns: computed on read, stored only in index, not in row data.
--    Compatible with both MySQL 5.7+ and MariaDB 10.2+.
--    Collision probability (birthday problem, P ≈ n² / (2 × 2^128)):
--      - 1 million paths:  ~10^-27
--      - 1 billion paths:  ~10^-21
--      - 1 trillion paths: ~10^-15
--    For context, other data loss risks are orders of magnitude higher:
--      - Enterprise SSD uncorrectable read error: ~10^-16 per bit
--      - S3 annual object loss probability:       ~10^-11 (11 nines)
--      - ECC DRAM uncorrectable error:            ~10^-12 per DIMM/hour
--    Even at 1 trillion paths, the MD5 collision probability is below the
--    hardware bit error rate. This is accepted as negligible data loss risk.
--    If a collision occurs: silent data loss (second row overwrites first via UPSERT).
--    No detection or logging is implemented — the overhead of a full-path comparison
--    on every UPSERT (row fetch on a 14-19K ops/sec hot path) is not justified to
--    protect against an event less likely than hardware failure.
--    Well before path cardinality approaches a scale where collisions matter, the correct
--    response is to partition the load across more tables — which the coordinator already
--    supports — not to upgrade the hash algorithm.
--
-- 6. File Size Columns Limited to 4GB
--    clp_ir_size_bytes and clp_archive_size_bytes are INT UNSIGNED (max 4GB).
--    Typical sizes: IR files are less than 1 MB, archives are 32-64 MB (CLP achieves high compression).
--    Accepted risk: 4GB limit is sufficient for expected workloads.
--    raw_size_bytes is BIGINT for uncompressed source files that may exceed 4GB.
--
-- 7. No Explicit created_at or updated_at Columns
--    The min_timestamp and max_timestamp semantics are application-defined.
--    For logging, they serve as both range bounds for split pruning
--    and approximate audit timestamps. For other use cases, explicit
--    created_at and updated_at columns may be needed.
--
-- 8. Index Overhead on Insert Performance
--    Base table has 7 indexes (PRIMARY, idx_id, idx_clp_ir_hash, idx_clp_archive_hash,
--    idx_consolidation, idx_expiration, idx_max_timestamp). Dimension indexes
--    are added dynamically by DynamicIndexManager. Each INSERT updates all indexes.
--    The VIRTUAL column indexes (MD5 hash) add computation overhead.
--    Mitigation: Batch inserts amortize index update cost. With batch size
--    5000 and rewriteBatchedStatements=true, expect ~5,500 records/sec.
--    See docs/operations/performance-tuning.md for JDBC tuning and batch size guidance.
--
--    Scaling note: Each table serves as a logical shard for a subset of
--    grouped datasets. Per-table volume is a couple hundred million rows
--    (up to ~1 billion across all tables), with 30-day retention. Aggregate
--    ingestion rate is couple hundred million to 1 billion files per day
--    across all tables. Daily RANGE partitioning limits the hot working set:
--    UPSERTs only touch the current day's partition, so the InnoDB buffer
--    pool only needs to hold one partition's indexes. Queries with
--    min_timestamp predicates get partition pruning and never scan cold
--    indexes. At a couple hundred million rows per table, index pressure
--    is not a practical concern. If a single DB instance reaches capacity,
--    additional instances can be added — each handling a subset of tables.
--    No cross-table dependencies exist, so this is a clean operational split.
--
-- 9. Nullable Path Columns with Natural NULL Propagation
--    Path columns (clp_ir_path, clp_archive_path) are nullable, not empty string.
--    NULL propagates naturally: MD5(NULL) = NULL, UNHEX(NULL) = NULL.
--    This simplifies VIRTUAL column definitions (no CASE expression needed).
--    Benefits:
--      - IR path NULL: allows archive-only entries without UNIQUE constraint conflicts
--      - Archive path NULL: saves ~10 bytes/row index space for pre-consolidation rows
--      - Cleaner semantics: NULL means "not set", non-NULL means "has value"
--    Query patterns:
--      - Find by path: WHERE clp_ir_path_hash = UNHEX(MD5(?))
--      - Find unset: WHERE clp_ir_path IS NULL (or WHERE clp_ir_path_hash IS NULL)
--    Workflow inference from paths:
--      - ir=NULL, archive=<path>  → Archive-only
--      - ir=<path>, archive=<path> → HYBRID (consolidation complete)
--      - ir=<path>, archive=NULL   → IR-only OR HYBRID (pre-consolidation)
--    For the last case, check state column: IR_* states = IR-only, IR_ARCHIVE_* = HYBRID.
--
-- 10. Sketches SET Column Limited to 64 Members
--     MySQL SET type supports up to 64 distinct values (hard limit).
--     All 64 slots (s01-s64) are pre-allocated in the template DDL to avoid
--     ALTER TABLE MODIFY (which requires a table rebuild). The column is NULL
--     for most rows, so the 8-byte bitmask only applies to rows with sketches.
--     If 64 sketch fields are exhausted, the design must be revisited.
--
-- 11. All Timestamps Are BIGINT (Epoch Nanoseconds)
--     Every timestamp column across all tables uses BIGINT epoch nanoseconds.
--     This includes both file-level timestamps (min_timestamp, max_timestamp,
--     clp_archive_created_at, expires_at) and operational timestamps
--     (created_at, claimed_at, heartbeat_at, etc. in task queue, node registry,
--     column registries, and table assignment).
--     Rationale: uniform units eliminate unit-mismatch bugs when joining or
--     comparing timestamps across tables. The storage overhead for operational
--     tables is negligible (tens to low thousands of rows). Nanosecond
--     precision aligns with CLP's internal timestamps and protobuf int64 wire
--     types. No DEFAULT (UNIX_TIMESTAMP()) is used — the application must
--     supply time.Now().UnixNano() on insert to ensure correct units.
--
-- 12. State Transition Enforcement Is Application-Layer Only
--     The state ENUM constrains the set of valid values, but valid transitions
--     (e.g., IR_BUFFERING can only go to IR_CLOSED) are enforced in metastore/model.go,
--     not via CHECK constraints or BEFORE UPDATE triggers.
--     Design decision: CHECK constraints (MariaDB 10.2+, MySQL 8.0.16+) can validate
--     column values but cannot reference OLD.state vs NEW.state — they see only the new
--     row. Transition validation requires a BEFORE UPDATE trigger that compares OLD and
--     NEW, adding a function call on every UPSERT in a table doing 14-19K ops/sec.
--     All state transitions go through FileRecords (metastore/filerecords.go), which uses guarded UPSERTs
--     (IF(UPSERT_GUARD, ...)) that already prevent regressions. The application is the
--     only writer; there is no direct SQL access in production. Adding trigger overhead
--     to defend against a scenario that the architecture already prevents is not justified.
--
-- 13. No Versioned Migration Framework (Flyway, Liquibase)
--     Base schema changes are rare (annually at most) and applied manually via online DDL.
--     A _schema_version table signals migration completion to the application, allowing it
--     to branch behavior during long-running online ALTER TABLE operations. Versioned
--     migration frameworks are not used because the base schema is stable — dynamic schema
--     evolution (dimensions, aggregations, sketches) is handled entirely by ColumnRegistry
--     via online DDL, which is the high-frequency evolution path. For the rare base schema
--     change, manual intervention with operator verification is sufficient and avoids adding
--     a library dependency and migration infrastructure for an event that may never occur.
--
-- ============================================================================
-- REFERENCES
-- ============================================================================
--
-- Design: ../../../../docs/concepts/metadata-schema.md (schema design)
-- Tables: ../../../../docs/reference/metadata-tables.md (DDL, column, index reference)
-- Performance: ../../../../docs/operations/performance-tuning.md (JDBC settings, batch sizes)
-- Schema evolution: ../../../../docs/guides/evolve-schema.md (ColumnRegistry, online DDL)
-- Column registry: schema/columnregistry.go (placeholder name mapping)
-- Partition management: schema/partitionmanager.go (lifecycle automation)
--

-- ============================================================================
-- TABLE REGISTRY (4 sub-tables with 1:1 relationships)
-- ============================================================================
--
-- Split into identity, Kafka routing, feature config, and node assignment.
-- Each sub-table uses table_name as PK/FK for 1:1 relationships.
--
-- WHY 4 TABLES INSTEAD OF 1?
--
-- The split is driven by write cadence and deployment topology, not entity
-- modeling theory. Each sub-table is written at a different frequency and
-- potentially by different nodes:
--
--   _table            Stable identity. Written once at provisioning, rarely
--                     changed. Anchor for all FKs.
--
--   _table_kafka      Environment-specific routing (bootstrap servers, topic).
--                     Different coordinators in dev/prod/staging can point to
--                     different Kafka clusters for the same logical table.
--                     Updated during Kafka migrations, not during normal ops.
--
--   _table_config     Feature flags and operational parameters. Read-heavy,
--                     hot-reloaded by coordinators. Rarely written (operator
--                     config changes). Must not contend with assignment writes.
--
--   _table_assignment Hot table. Heartbeats, lease expiry, and progress
--                     timestamps updated every few seconds by whichever
--                     coordinator currently owns the table. Coordinators are
--                     stateless — any node can take over on restart — so the
--                     assignment row is the lightweight lease mechanism.
--                     Keeping it narrow minimizes the critical section during
--                     failover.
--
-- A single wide row would mean every heartbeat UPDATE touches a row that
-- config reads also need, creating unnecessary InnoDB row-lock contention.
--
-- FK CASCADE POLICY:
-- All child tables use ON DELETE CASCADE. Deleting a row from _table
-- automatically cleans up config, assignment, Kafka routing, registries,
-- and tasks. This is safe because table decommissioning is a rare,
-- operator-initiated action, and CASCADE eliminates the risk of orphaned
-- rows across 7 child tables. The active flag on _table supports
-- soft-delete for normal operations; hard delete is reserved for full
-- decommissioning.
--
-- ON UPDATE CASCADE is intentionally omitted. table_name is an immutable
-- identifier — table renames are not supported. Renaming would require
-- updating the cloned data table name, all Kafka consumer group IDs,
-- storage paths, and monitoring dashboards. If a name change is needed,
-- provision a new table and migrate data.
--
-- See: ../../../../docs/design/coordinator-ha.md
--

-- Identity: slim registry of managed tables.
--
-- table_id (UUID):  Used to build globally unique Kafka consumer group IDs
--   (e.g., clp-coordinator-{table_name}-{table_id}) so that multiple
--   environments (dev, staging, prod) consuming the same Kafka topic
--   maintain independent offsets — even when coordinators fail over to
--   different nodes via HA. NOT used as a FK; table_name is the FK
--   everywhere for human-readable queries, logs, and debugging.
CREATE TABLE IF NOT EXISTS _table (
    table_id      CHAR(36) NOT NULL DEFAULT (UUID()) PRIMARY KEY,
    table_name    VARCHAR(64) NOT NULL UNIQUE,
    display_name  VARCHAR(128) NOT NULL,
    active        BOOLEAN NOT NULL DEFAULT TRUE
) ENGINE=InnoDB;

-- Kafka routing: bootstrap servers, topic, and message transformer per table
CREATE TABLE IF NOT EXISTS _table_kafka (
    table_name              VARCHAR(64) NOT NULL PRIMARY KEY,
    kafka_bootstrap_servers VARCHAR(255) NOT NULL DEFAULT 'localhost:9092',
    kafka_topic             VARCHAR(255) NOT NULL,
    record_transformer      VARCHAR(64) NULL,
    FOREIGN KEY (table_name) REFERENCES _table(table_name) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Feature config: typed columns instead of JSON blob.
-- Note: DDL DEFAULT values are used for columns not explicitly set by INSERT.
-- The TableProvisioner (tableprovisioner.go) may override specific defaults
-- (e.g., kafka_poller_enabled defaults to TRUE here but is inserted as FALSE
-- during table provisioning). Check both DDL defaults and insertRegistryRows()
-- when changing defaults.
CREATE TABLE IF NOT EXISTS _table_config (
    table_name                          VARCHAR(64) NOT NULL PRIMARY KEY,
    kafka_poller_enabled                BOOLEAN NOT NULL DEFAULT TRUE,
    metadata_writer_enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    consolidation_enabled               BOOLEAN NOT NULL DEFAULT TRUE,
    retention_cleanup_enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    FOREIGN KEY (table_name) REFERENCES _table(table_name) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Node assignment: which node owns this table
-- NULL = unassigned. Claimed via UPDATE ... WHERE node_id = <old>.
CREATE TABLE IF NOT EXISTS _table_assignment (
    table_name            VARCHAR(64) NOT NULL PRIMARY KEY,
    node_id               VARCHAR(64) NULL,
    node_assigned_at      BIGINT NULL,
    assignment_updated_at BIGINT NULL,
    last_progress_at      BIGINT NULL,
    lease_expiry          BIGINT NULL,
    INDEX idx_node_id (node_id),
    FOREIGN KEY (table_name) REFERENCES _table(table_name) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Node registry: tracks live nodes via heartbeat for orphan detection
-- Nodes update their heartbeat timestamp periodically; missing or stale
-- entries indicate dead nodes whose tables can be claimed by others.
-- See: ../../../../docs/design/coordinator-ha.md
CREATE TABLE IF NOT EXISTS _node_registry (
    node_id           VARCHAR(64) PRIMARY KEY,
    last_heartbeat_at BIGINT NOT NULL,
    started_at        BIGINT NOT NULL,
    INDEX idx_heartbeat (last_heartbeat_at)
) ENGINE=InnoDB;


CREATE TABLE IF NOT EXISTS _clp_template (

    -- ========================================================================
    -- IDENTITY AND TIME
    -- ========================================================================

    id                          BIGINT AUTO_INCREMENT,
    min_timestamp               BIGINT NOT NULL,                      -- Partition key (earliest event epoch nanos)
    max_timestamp               BIGINT NOT NULL DEFAULT 0,            -- Latest event epoch nanos
    clp_archive_created_at      BIGINT NOT NULL DEFAULT 0,            -- Archive creation epoch nanos

    -- ========================================================================
    -- STORAGE LOCATIONS
    -- ========================================================================
    --
    -- S3 URL: s3://bucket-name/path/to/object.ir
    --   - bucket: maximum 63 characters, ASCII only (a-z, 0-9, hyphens, periods)
    --   - path (object key): maximum 1024 bytes UTF-8
    --

    -- IR file location (NULL indicates archive-only entry)
    -- Path columns are for projection only; use hash index for lookups
    -- Accepts any @StorageBackendType value (e.g., "s3", "minio", "local"). NULL = not set.
    clp_ir_storage_backend      VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    clp_ir_bucket               VARCHAR(63) CHARACTER SET ascii COLLATE ascii_bin NULL,
    clp_ir_path                 VARCHAR(1024) NULL,

    -- Archive location (NULL indicates not yet consolidated)
    -- Path columns are for projection only; use hash index for lookups
    clp_archive_storage_backend VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    clp_archive_bucket          VARCHAR(63) CHARACTER SET ascii COLLATE ascii_bin NULL,
    clp_archive_path            VARCHAR(1024) NULL,

    -- ========================================================================
    -- VIRTUAL COLUMNS FOR HASH-BASED LOOKUPS
    -- ========================================================================
    --
    -- VIRTUAL columns: computed on read, stored only in index, not in row data.
    -- Compatible with both MySQL 5.7+ and MariaDB 10.2+.
    -- NULL propagates naturally: MD5(NULL) = NULL, UNHEX(NULL) = NULL
    --   - IR path NULL: allows archive-only entries without uniqueness conflicts
    --   - Archive path NULL: saves index space for pre-consolidation rows
    --

    clp_ir_path_hash            BINARY(16) AS (UNHEX(MD5(clp_ir_path))) VIRTUAL,
    clp_archive_path_hash       BINARY(16) AS (UNHEX(MD5(clp_archive_path))) VIRTUAL,

    -- ========================================================================
    -- LIFECYCLE STATE
    -- ========================================================================

    -- Three lifecycle chains — a file enters ONE chain at creation:
    --
    --   IR-only chain (no consolidation):
    --     IR_BUFFERING → IR_CLOSED → IR_PURGING
    --
    --   Archive-only chain (pre-built archives, no IR stage):
    --     ARCHIVE_CLOSED → ARCHIVE_PURGING
    --
    --   Hybrid chain (IR ingested, then consolidated into archive):
    --     IR_ARCHIVE_BUFFERING → IR_ARCHIVE_CONSOLIDATION_PENDING → ARCHIVE_CLOSED → ARCHIVE_PURGING
    --
    -- The hybrid and archive-only chains share the ARCHIVE_CLOSED →
    -- ARCHIVE_PURGING tail. The starting state is chosen by the producer
    -- at file creation time. There is NO transition between the IR-only
    -- chain and the other chains (e.g., IR_BUFFERING cannot become
    -- IR_ARCHIVE_BUFFERING or ARCHIVE_CLOSED).
    state ENUM(
        -- IR-only chain (ordinals 1-3, lifecycle order)
        'IR_BUFFERING',                      -- IR file still being written (entry point)
        'IR_CLOSED',                         -- IR file closed, queryable
        'IR_PURGING',                        -- IR file scheduled for deletion

        -- Hybrid chain (ordinals 4-5, lifecycle order)
        -- Continues into archive chain below after consolidation
        'IR_ARCHIVE_BUFFERING',              -- IR file being written, will be consolidated (entry point)
        'IR_ARCHIVE_CONSOLIDATION_PENDING',  -- IR closed, awaiting consolidation → ARCHIVE_CLOSED

        -- Archive chain (ordinals 6-7, lifecycle order)
        -- Shared tail for both archive-only and hybrid chains
        'ARCHIVE_CLOSED',                    -- Archive created (entry point for archive-only, or post-consolidation)
        'ARCHIVE_PURGING'                    -- Archive scheduled for deletion
    ) NOT NULL,

    -- ========================================================================
    -- METRICS
    -- ========================================================================

    record_count                INT UNSIGNED NOT NULL DEFAULT 0,      -- Total log records
    raw_size_bytes              BIGINT NULL,                          -- Original size (can exceed 4GB)
    clp_ir_size_bytes           INT UNSIGNED NULL,                    -- IR file size
    clp_archive_size_bytes      INT UNSIGNED NULL,                    -- Archive size

    -- ========================================================================
    -- RETENTION
    -- ========================================================================

    -- retention_days: default policy (how long to keep data).
    -- expires_at: effective expiration, initially min_timestamp + retention_days.
    -- These are independent — expires_at may be extended manually (e.g., after a
    -- security incident) without changing the policy. Do not recompute expires_at
    -- from retention_days; treat it as the authoritative expiration.
    retention_days              SMALLINT UNSIGNED NOT NULL DEFAULT 30,
    expires_at                  BIGINT NOT NULL DEFAULT 0,

    -- ========================================================================
    -- DIMENSIONS (Dynamic)
    -- ========================================================================
    --
    -- Dimension columns are added dynamically via ColumnRegistry.
    -- Physical names use opaque placeholders: dim_f01, dim_f02, ...
    -- Logical field names are stored in _dim_registry.dim_key.
    --
    -- See schema/columnregistry.go for automatic column creation and
    -- docs/guides/evolve-schema.md for the placeholder naming design.
    --

    -- ========================================================================
    -- SKETCHES (Probabilistic Membership Filters)
    -- ========================================================================
    --
    -- SET column indicates which fields have data sketches (bloom/cuckoo filters) in ext.
    -- NULL for most rows (no sketches); non-NULL when sketches are available.
    -- All 64 SET members are pre-allocated to avoid ALTER TABLE MODIFY, which
    -- requires a table rebuild. The column is NULL for most rows, so the 8-byte
    -- bitmask storage only applies to rows that actually have sketches.
    -- MySQL SET supports up to 64 members (hard limit).
    --
    -- Usage: sketches IS NOT NULL AND FIND_IN_SET('user_id', sketches) > 0
    --

    sketches                    SET('s01','s02','s03','s04','s05','s06','s07','s08',
                                    's09','s10','s11','s12','s13','s14','s15','s16',
                                    's17','s18','s19','s20','s21','s22','s23','s24',
                                    's25','s26','s27','s28','s29','s30','s31','s32',
                                    's33','s34','s35','s36','s37','s38','s39','s40',
                                    's41','s42','s43','s44','s45','s46','s47','s48',
                                    's49','s50','s51','s52','s53','s54','s55','s56',
                                    's57','s58','s59','s60','s61','s62','s63','s64') NULL,

    -- ========================================================================
    -- EXTENSION DATA (Msgpack-encoded) - USE SPARINGLY
    -- ========================================================================
    --
    -- Escape hatch for opaque metadata. NULL for most rows.
    -- Primary use case: sketch data for split pruning.
    -- Not indexed—MySQL can't filter on contents; requires application-layer decoding.
    -- Msgpack for cross-database compatibility (MySQL and MariaDB).
    --
    -- If data is needed for queries or present on most files, add an explicit column instead.
    --

    ext                         MEDIUMBLOB NULL,

    -- ========================================================================
    -- KEYS AND INDEXES
    -- ========================================================================

    -- Primary key: partition key + auto-increment for write locality
    PRIMARY KEY (min_timestamp, id),

    -- Required for AUTO_INCREMENT on partitioned tables (must be first column in an index)
    KEY idx_id (id),

    -- IR path uniqueness via VIRTUAL column (per-partition due to MySQL constraint)
    -- NULL paths (archive-only entries) don't conflict in unique index
    -- Note: the guarded UPSERT reads state and max_timestamp after the duplicate
    -- key check. Including them as covering columns would avoid a row fetch on
    -- every UPSERT, but at the cost of a wider index (10 extra bytes/entry).
    -- At current volumes (~14-19K UPSERT/sec) the row fetch is not a bottleneck.
    UNIQUE KEY idx_clp_ir_hash (clp_ir_path_hash, min_timestamp),

    -- Archive path lookup via VIRTUAL column (non-unique: multiple IR files consolidate to one archive)
    -- NULL for pre-consolidation rows (no archive yet) saves index space
    INDEX idx_clp_archive_hash (clp_archive_path_hash),

    -- Dimension indexes are created dynamically by DynamicIndexManager
    -- when corresponding dimension columns are added.

    -- Operational indexes
    INDEX idx_consolidation (state, min_timestamp ASC),  -- Coordinator: pending files, oldest first
    INDEX idx_expiration (expires_at ASC),               -- Retention: expired files
    INDEX idx_max_timestamp (max_timestamp DESC)     -- Queries: time-range overlap

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_bin

-- ============================================================================
-- PARTITIONING
-- ============================================================================
--
-- Daily partitions by min_timestamp (event time, not ingestion time).
-- Coordinator creates partitions one week ahead and merges old sparse ones.
--
PARTITION BY RANGE (min_timestamp) (
    -- Historical catch-all (also merge target for old sparse partitions).
    -- In normal operation this partition stays near-empty: retention enforcement
    -- deletes expired files before their partitions are merged. The catch-all
    -- only accumulates data if retention is disabled, the deletion pipeline is
    -- broken, or a producer sends min_timestamp far in the past. All three are
    -- operational failures, not expected workload.
    PARTITION p_20240101 VALUES LESS THAN (1704067200000000000),

    -- Daily partitions created dynamically by PartitionManager
    -- Naming: p_YYYYMMDD (for example, p_20240115)

    -- Future catch-all (safety net, should rarely contain data)
    PARTITION p_future VALUES LESS THAN MAXVALUE
);


-- ============================================================================
-- PARTITION MANAGEMENT REFERENCE
-- ============================================================================
--
-- The coordinator's PartitionManager handles partition lifecycle automatically.
-- See node.yaml for configuration.
--
-- Create new partition (splits p_future):
--   ALTER TABLE _clp_template REORGANIZE PARTITION p_future INTO (
--       PARTITION p_20240115 VALUES LESS THAN (1705363200000000000),
--       PARTITION p_future VALUES LESS THAN MAXVALUE
--   );
--
-- Drop empty partition:
--   ALTER TABLE _clp_template DROP PARTITION p_20231201;
--
-- Merge sparse partition into historical catch-all:
--   ALTER TABLE _clp_template REORGANIZE PARTITION p_20240101, p_20240102 INTO (
--       PARTITION p_20240101 VALUES LESS THAN (1704153600000000000)
--   );
--
-- Query partition info:
--   SELECT PARTITION_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH
--   FROM INFORMATION_SCHEMA.PARTITIONS
--   WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '_clp_template'
--   ORDER BY PARTITION_ORDINAL_POSITION;


-- ============================================================================
-- TASK QUEUE TABLE
-- ============================================================================
--
-- Database-backed task queue for worker coordination. Workers poll this table
-- directly using FOR UPDATE SKIP LOCKED for distributed task claiming.
--
-- Design: ../../../../docs/design/task-queue.md
--
-- Flow:
--   1. PlannerThread inserts tasks with state='pending'
--   2. Workers claim tasks via SELECT ... FOR UPDATE SKIP LOCKED
--   3. Workers update state to 'processing', then 'completed' or 'failed'
--   4. PlannerThread reclaims stale tasks (processing too long)
--   5. PlannerThread cleans up old completed/failed/timed_out tasks
--
-- Recovery:
--   On coordinator restart, tasks are deleted and Kafka consumer group
--   (clp-coordinator-{table_name}-{table_id}) resumes from last committed offset.
--

CREATE TABLE IF NOT EXISTS _task_queue (
    task_id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name          VARCHAR(64) NOT NULL,
    state               ENUM('pending', 'processing', 'completed', 'failed', 'timed_out', 'dead_letter')
                        NOT NULL DEFAULT 'pending',
    -- No FK to _node_registry: workers may deregister before their tasks are
    -- reclaimed. Orphaned worker_id values are handled by stale task detection.
    worker_id           VARCHAR(64) NULL,
    created_at          BIGINT NOT NULL,
    claimed_at          BIGINT NULL,
    completed_at        BIGINT NULL,
    retry_count         TINYINT UNSIGNED NOT NULL DEFAULT 0,
    -- MEDIUMBLOB (16 MB max) is used instead of BLOB (64 KB) to avoid a
    -- table-rebuild ALTER if payloads ever grow beyond 64 KB. Typical payloads
    -- are LZ4+msgpack-encoded file path lists, well under 64 KB. The application
    -- layer should enforce a reasonable size limit (e.g., 1 MB) before insert.
    input               MEDIUMBLOB NOT NULL,
    output              MEDIUMBLOB NULL,

    -- Claim: Find pending tasks for a table, ordered by task_id (FIFO)
    -- Note: ORDER BY task_id uses implicit PK appended to secondary index
    INDEX idx_claim (table_name, state),

    -- Stale detection: Find processing tasks past timeout
    INDEX idx_stale (table_name, state, claimed_at),

    -- Cleanup: Find old completed/failed/timed_out tasks
    INDEX idx_cleanup (table_name, state, completed_at),

    -- worker_id is intentionally not indexed. Stale task detection uses
    -- idx_stale (by table + state + claimed_at), not by worker_id.
    -- The task queue is small (hundreds to low thousands of rows per table)
    -- and cleaned up aggressively, so a worker_id index would add write
    -- overhead without meaningful query benefit.

    FOREIGN KEY (table_name) REFERENCES _table(table_name) ON DELETE CASCADE
) ENGINE=InnoDB;


-- ============================================================================
-- COLUMN REGISTRY TABLES
-- ============================================================================
--
-- Maps opaque placeholder column names (dim_f01, agg_f01) to real field
-- metadata. Enables:
--   1. Field names with special characters (@, ., -, /)
--   2. Short column names (dim_f01 vs dim_str128_application_id)
--   3. Column recycling (ACTIVE -> INVALIDATED -> AVAILABLE)
--
-- See: ../../../../docs/guides/evolve-schema.md
--

-- Dimension column registry: maps dim_fNN placeholders to field metadata
CREATE TABLE IF NOT EXISTS _dim_registry (
    table_name      VARCHAR(64) NOT NULL,
    column_name     VARCHAR(64) NOT NULL,
    base_type       ENUM('str','str_utf8','bool','int','float') NOT NULL,
    width           SMALLINT UNSIGNED NULL,
    dim_key         VARCHAR(1024) NOT NULL,
    alias_column    VARCHAR(64)   NULL,
    state           ENUM('ACTIVE','INVALIDATED','AVAILABLE') NOT NULL,
    created_at      BIGINT NOT NULL,
    invalidated_at  BIGINT NULL,
    PRIMARY KEY (table_name, column_name),
    INDEX idx_dim_lookup (table_name, dim_key(255), state),
    FOREIGN KEY (table_name) REFERENCES _table(table_name) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Aggregate column registry: maps agg_fNN placeholders to field metadata
CREATE TABLE IF NOT EXISTS _agg_registry (
    table_name        VARCHAR(64)   NOT NULL,
    column_name       VARCHAR(64)   NOT NULL,
    agg_key           VARCHAR(1024) NOT NULL,
    agg_value         VARCHAR(1024) NULL,
    aggregation_type  ENUM('EQ','GTE','GT','LTE','LT','SUM','AVG','MIN','MAX') NOT NULL DEFAULT 'EQ',
    value_type        ENUM('INT','FLOAT') NOT NULL DEFAULT 'INT',
    alias_column      VARCHAR(64)   NULL,
    state             ENUM('ACTIVE','INVALIDATED','AVAILABLE') NOT NULL,
    created_at        BIGINT  NOT NULL,
    invalidated_at    BIGINT  NULL,
    PRIMARY KEY (table_name, column_name),
    INDEX idx_agg_lookup (table_name, agg_key(255), state),
    FOREIGN KEY (table_name) REFERENCES _table(table_name) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Sketch registry: maps SET members (s01..s32) to field metadata
-- Slots are pre-populated as AVAILABLE during table registration
CREATE TABLE IF NOT EXISTS _sketch_registry (
    table_name      VARCHAR(64) NOT NULL,
    sketch_name     VARCHAR(8) NOT NULL,
    sketch_key      VARCHAR(1024) NULL,
    state           ENUM('ACTIVE','INVALIDATED','AVAILABLE') NOT NULL,
    created_at      BIGINT NOT NULL,
    invalidated_at  BIGINT NULL,
    PRIMARY KEY (table_name, sketch_name),
    FOREIGN KEY (table_name) REFERENCES _table(table_name) ON DELETE CASCADE
) ENGINE=InnoDB;
