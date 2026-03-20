// Package retention implements pluggable retention strategies for file
// lifecycle management. A [Strategy] runs as a background goroutine within
// a coordinator unit, periodically scanning for expired files and deleting
// them from both the database and object storage.
//
// Custom strategies are registered via [RegisterType] and selected per-table
// through the retention.type field in _table_config. The built-in "default"
// strategy scans for files past their expires_at timestamp.
package retention
