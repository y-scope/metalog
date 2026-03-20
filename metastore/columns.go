package metastore

// Column name constants matching the schema.sql _clp_template table.
// These are the "base" columns that exist on every table.
const (
	ColID                       = "id"
	ColMinTimestamp             = "min_timestamp"
	ColMaxTimestamp             = "max_timestamp"
	ColClpArchiveCreatedAt      = "clp_archive_created_at"
	ColClpIRStorageBackend      = "clp_ir_storage_backend"
	ColClpIRBucket              = "clp_ir_bucket"
	ColClpIRPath                = "clp_ir_path"
	ColClpArchiveStorageBackend = "clp_archive_storage_backend"
	ColClpArchiveBucket         = "clp_archive_bucket"
	ColClpArchivePath           = "clp_archive_path"
	ColClpIRPathHash            = "clp_ir_path_hash"
	ColClpArchivePathHash       = "clp_archive_path_hash"
	ColState                    = "state"
	ColRecordCount              = "record_count"
	ColRawSizeBytes             = "raw_size_bytes"
	ColClpIRSizeBytes           = "clp_ir_size_bytes"
	ColClpArchiveSizeBytes      = "clp_archive_size_bytes"
	ColRetentionDays            = "retention_days"
	ColExpiresAt                = "expires_at"
	ColSketches                 = "sketches"
	ColExt                      = "ext"
)

// DefaultRetentionDays is the default retention period when the producer does not specify one.
const DefaultRetentionDays = 30

// baseCols returns the fixed columns used in standard INSERT/UPSERT operations.
// Order matters — these correspond to the VALUES placeholders.
// Returns a fresh slice each call to prevent accidental mutation.
func baseCols() []string {
	return []string{
		ColMinTimestamp,
		ColMaxTimestamp,
		ColClpArchiveCreatedAt,
		ColClpIRStorageBackend,
		ColClpIRBucket,
		ColClpIRPath,
		ColClpArchiveStorageBackend,
		ColClpArchiveBucket,
		ColClpArchivePath,
		ColState,
		ColRecordCount,
		ColRawSizeBytes,
		ColClpIRSizeBytes,
		ColClpArchiveSizeBytes,
		ColRetentionDays,
		ColExpiresAt,
		ColSketches,
		ColExt,
	}
}

// guardedUpdateCols returns the columns that use IF(guard, ...) in ON DUPLICATE KEY UPDATE.
// Returns a fresh slice each call to prevent accidental mutation.
func guardedUpdateCols() []string {
	return []string{
		ColMaxTimestamp,
		ColClpArchiveCreatedAt,
		ColClpIRStorageBackend,
		ColClpIRBucket,
		ColClpIRPath,
		ColClpArchiveStorageBackend,
		ColClpArchiveBucket,
		ColClpArchivePath,
		ColState,
		ColRecordCount,
		ColRawSizeBytes,
		ColClpIRSizeBytes,
		ColClpArchiveSizeBytes,
		ColRetentionDays,
		ColExpiresAt,
		ColSketches,
		ColExt,
	}
}

// Table registry table names.
const (
	TableRegistry       = "_table"
	TableRegistryConfig = "_table_config"
	TableRegistryAssignment = "_table_assignment"
	NodeRegistryTable       = "_node_registry"
	DimRegistryTable        = "_dim_registry"
	AggRegistryTable        = "_agg_registry"
	SketchRegistryTable     = "_sketch_registry"
	TemplateTable           = "_clp_template"
)

// DimColumnPrefix is the prefix for dynamically allocated dimension columns.
const DimColumnPrefix = "dim_f"

// AggColumnPrefix is the prefix for dynamically allocated aggregation columns.
const AggColumnPrefix = "agg_f"
