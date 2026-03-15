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
//   - [SparkJobPolicy]: groups files by a dimension key (e.g., application_id)
//
// Archive paths are generated per-group via [GenerateArchivePath] (UUIDv7-based).
// Policies may override this to produce custom paths.
package consolidation
