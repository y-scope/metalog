package metastore

import (
	"database/sql"
)

// FileState represents the lifecycle state of a metadata file record.
type FileState string

const (
	StateIRBuffering                   FileState = "IR_BUFFERING"
	StateIRClosed                      FileState = "IR_CLOSED"
	StateIRPurging                     FileState = "IR_PURGING"
	StateArchiveClosed                 FileState = "ARCHIVE_CLOSED"
	StateArchivePurging                FileState = "ARCHIVE_PURGING"
	StateIRArchiveBuffering            FileState = "IR_ARCHIVE_BUFFERING"
	StateIRArchiveConsolidationPending FileState = "IR_ARCHIVE_CONSOLIDATION_PENDING"
)

// String implements fmt.Stringer.
func (s FileState) String() string { return string(s) }

// IsTerminal returns true for states that represent a completed lifecycle.
func (s FileState) IsTerminal() bool {
	return s == StateIRPurging || s == StateArchivePurging
}

// CanTransitionTo returns true if transitioning from s to target is valid.
func (s FileState) CanTransitionTo(target FileState) bool {
	switch s {
	case StateIRBuffering:
		return target == StateIRClosed
	case StateIRClosed:
		return target == StateIRPurging
	case StateIRArchiveBuffering:
		return target == StateIRArchiveConsolidationPending
	case StateIRArchiveConsolidationPending:
		return target == StateArchiveClosed
	case StateArchiveClosed:
		return target == StateArchivePurging
	default:
		return false
	}
}

// UpsertGuardStates are states that should NOT be overwritten by an UPSERT.
// If a record is in one of these states, the guarded UPSERT preserves the
// existing row instead of applying new values.
var UpsertGuardStates = []FileState{
	StateIRPurging,
	StateIRArchiveConsolidationPending,
	StateArchiveClosed,
	StateArchivePurging,
}

// FileRecord represents a single row in a metadata table.
type FileRecord struct {
	ID                       int64
	MinTimestamp             int64
	MaxTimestamp             int64
	ClpArchiveCreatedAt      int64
	ClpIRStorageBackend      sql.NullString
	ClpIRBucket              sql.NullString
	ClpIRPath                sql.NullString
	ClpArchiveStorageBackend sql.NullString
	ClpArchiveBucket         sql.NullString
	ClpArchivePath           sql.NullString
	State                    FileState
	RecordCount              uint32
	RawSizeBytes             sql.NullInt64
	ClpIRSizeBytes           sql.NullInt32
	ClpArchiveSizeBytes      sql.NullInt32
	RetentionDays            uint16
	ExpiresAt                int64

	// Dynamic columns: dim_fNN values keyed by column name.
	Dims map[string]any
	// Dynamic columns: agg_fNN values keyed by column name.
	Aggs map[string]any

	// Flushed receives nil after this record has been successfully written to the
	// database, or an error if the flush failed. Used by the Kafka consumer to
	// know when it's safe to commit offsets. Buffered (cap 1) so the writer
	// never blocks.
	Flushed chan error
}

// DeletionResult holds the outcome of a file deletion batch.
type DeletionResult struct {
	IRPaths      []StoragePath
	ArchivePaths []StoragePath
	DeletedCount int64
}

// StoragePath identifies a file in object storage.
type StoragePath struct {
	Backend string
	Bucket  string
	Path    string
}
