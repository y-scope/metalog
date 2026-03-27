package metastore

import (
	"database/sql"
	"fmt"
)

// FileState represents the lifecycle state of a metadata file record.
//
// Three independent lifecycle chains — a file enters ONE chain at creation
// and never crosses to another:
//
//	IR-only chain:      IR_BUFFERING → IR_CLOSED → IR_PURGING
//	Archive-only chain: ARCHIVE_CLOSED → ARCHIVE_PURGING
//	Hybrid chain:       IR_ARCHIVE_BUFFERING → IR_ARCHIVE_CONSOLIDATION_PENDING → ARCHIVE_CLOSED → ARCHIVE_PURGING
//
// The starting state is chosen by the producer at file creation time.
type FileState string

const (
	// IR-only chain
	StateIRBuffering FileState = "IR_BUFFERING"
	StateIRClosed    FileState = "IR_CLOSED"
	StateIRPurging   FileState = "IR_PURGING"

	// Hybrid chain
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

// UpsertGuardStates returns states that should NOT be overwritten by an UPSERT.
// If a record is in one of these states, the guarded UPSERT preserves the
// existing row instead of applying new values.
// Returns a fresh slice each call to prevent accidental mutation.
func UpsertGuardStates() []FileState {
	return []FileState{
		StateIRPurging,
		StateIRArchiveConsolidationPending,
		StateArchiveClosed,
		StateArchivePurging,
	}
}

// ValidStates returns all recognized FileState values.
func ValidStates() []FileState {
	return []FileState{
		StateIRBuffering, StateIRClosed, StateIRPurging,
		StateIRArchiveBuffering, StateIRArchiveConsolidationPending,
		StateArchiveClosed, StateArchivePurging,
	}
}

// stateRequiresIR returns true if the state requires a non-empty IR path.
func stateRequiresIR(s FileState) bool {
	return s == StateIRBuffering || s == StateIRClosed ||
		s == StateIRArchiveBuffering || s == StateIRArchiveConsolidationPending
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
	ClpIRSizeBytes           sql.NullInt64
	ClpArchiveSizeBytes      sql.NullInt64
	RetentionDays            uint16
	ExpiresAt                int64

	// Dynamic columns: keyed by logical name during ingestion, remapped to
	// physical column names (dim_fNN / agg_fNN) at batch flush time.
	Dims map[string]any
	Aggs map[string]any

	// Schema evolution metadata: carried from proto extraction to batch flush
	// so the BatchingWriter can resolve/allocate physical columns.
	DimMeta []DimMeta
	AggMeta []AggMeta

	// Sketch data: keyed by logical field name (e.g. "uuid"), values are
	// msgpack-encoded BloomFilterSnapshot bytes from clp-ffi-go.
	Sketches map[string][]byte

	// Populated at batch flush time after sketch registry resolution.
	// SketchSetValue is the comma-joined SET members (e.g. "s01,s03").
	// ExtData is the LZ4-compressed msgpack ext blob containing sketch data.
	SketchSetValue string
	ExtData        []byte

	// Flushed receives nil after this record has been successfully written to the
	// database, or an error if the flush failed. Used by the Kafka consumer to
	// know when it's safe to commit offsets. Buffered (cap 1) so the writer
	// never blocks.
	Flushed chan error
}

// Validate checks that a FileRecord has all required fields and valid values.
func (r *FileRecord) Validate() error {
	if r.State == "" {
		return fmt.Errorf("state is required")
	}
	valid := false
	for _, s := range ValidStates() {
		if r.State == s {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("invalid state: %q", r.State)
	}
	if r.MinTimestamp == 0 {
		return fmt.Errorf("min_timestamp is required")
	}
	if r.MaxTimestamp != 0 && r.MaxTimestamp < r.MinTimestamp {
		return fmt.Errorf("max_timestamp (%d) must be >= min_timestamp (%d)", r.MaxTimestamp, r.MinTimestamp)
	}
	if stateRequiresIR(r.State) && !r.ClpIRPath.Valid {
		return fmt.Errorf("clp_ir_path is required for state %s", r.State)
	}
	return nil
}

// DimMeta carries dimension type info needed for schema evolution (ALTER TABLE ADD COLUMN).
type DimMeta struct {
	Key      string // logical dim key (e.g. "host")
	BaseType string // str, str_utf8, bool, int, float
	Width    int    // column width hint for string types
}

// AggMeta carries aggregation type info needed for schema evolution.
type AggMeta struct {
	Key       string // agg field name (e.g. "level")
	Value     string // qualifier (e.g. "error")
	Type      string // aggregation type: EQ, GTE, SUM, etc.
	ValueType string // INT or FLOAT
	AliasCol  string // optional alias column
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
