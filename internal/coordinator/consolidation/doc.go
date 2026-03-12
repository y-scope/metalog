// Package consolidation implements the archive consolidation planner and
// policies for metalog.
//
// The [Planner] periodically scans for IR files ready for consolidation,
// applies a [Policy] to group them into consolidation tasks, and enqueues
// those tasks into the database task queue. It uses an [InFlightSet] to
// avoid re-planning tasks that are already in progress.
//
// Built-in policies:
//   - [TimeWindowPolicy]: groups files by time window boundaries
//   - [SparkJobPolicy]: groups files for Spark-based consolidation
//   - [AuditPolicy]: logs candidates without creating tasks (dry-run)
//
// The [ArchivePathGenerator] creates deterministic archive paths from
// table name and timestamp range.
package consolidation
