// Package schema manages the physical database schema for metadata tables.
//
// It provisions tables from a template ([EnsureTable]), manages RANGE partitions
// by timestamp ([PartitionManager]), maintains secondary indexes ([IndexManager]),
// and tracks dynamic column allocation via the [ColumnRegistry].
//
// The column registry maps user-facing field names to physical dim_fNN/agg_fNN
// columns, supporting thread-safe slot allocation and atomic snapshots for
// concurrent readers.
package schema
