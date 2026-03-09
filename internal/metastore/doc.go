// Package metastore implements the metadata data model and database operations
// for CLP file records.
//
// It defines the [FileRecord] type, [FileState] lifecycle, column constants, and
// the [FileRecords] repository for batch UPSERT, state transitions, and queries.
// The guarded UPSERT logic (see [BuildGuardedUpsertSQL]) ensures idempotent,
// monotonic ingestion that never overwrites records in protected states.
//
// Advisory locks ([AdvisoryLock]) provide cross-process coordination for
// operations like consolidation planning that must be globally serialized.
package metastore
