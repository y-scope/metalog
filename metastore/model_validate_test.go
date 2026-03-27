package metastore

import (
	"database/sql"
	"testing"
)

func TestFileRecord_Validate_Success(t *testing.T) {
	r := &FileRecord{
		State:        StateIRBuffering,
		MinTimestamp:  1000,
		MaxTimestamp:  2000,
		ClpIRPath:    sql.NullString{Valid: true, String: "/path/to/ir"},
	}
	if err := r.Validate(); err != nil {
		t.Errorf("valid record failed: %v", err)
	}
}

func TestFileRecord_Validate_BadState(t *testing.T) {
	r := &FileRecord{State: "INVALID"}
	if err := r.Validate(); err == nil {
		t.Error("expected error for invalid state")
	}
}

func TestFileRecord_Validate_MissingIRPath(t *testing.T) {
	r := &FileRecord{
		State:       StateIRBuffering,
		MinTimestamp: 1000,
		MaxTimestamp: 2000,
	}
	if err := r.Validate(); err == nil {
		t.Error("expected error for missing IR path in IR state")
	}
}

func TestFileRecord_Validate_BadTimestamps(t *testing.T) {
	r := &FileRecord{
		State:        StateArchiveClosed,
		MinTimestamp:  2000,
		MaxTimestamp:  1000,
	}
	if err := r.Validate(); err == nil {
		t.Error("expected error for min > max timestamp")
	}
}

func TestFileState_String(t *testing.T) {
	if s := StateIRBuffering.String(); s != string(StateIRBuffering) {
		t.Errorf("String() = %q", s)
	}
}

func TestValidStates(t *testing.T) {
	states := ValidStates()
	if len(states) != 7 {
		t.Errorf("ValidStates() has %d states, want 7", len(states))
	}
}

func TestStateRequiresIR(t *testing.T) {
	if !stateRequiresIR(StateIRBuffering) {
		t.Error("IR_BUFFERING should require IR")
	}
	if stateRequiresIR(StateArchiveClosed) {
		t.Error("ARCHIVE_CLOSED should not require IR")
	}
}

func TestBaseCols(t *testing.T) {
	cols := baseCols()
	if len(cols) == 0 {
		t.Fatal("baseCols() should not be empty")
	}
	cols2 := baseCols()
	cols[0] = "modified"
	if cols2[0] == "modified" {
		t.Error("baseCols() should return a fresh slice")
	}
}

func TestGuardedUpdateCols(t *testing.T) {
	cols := guardedUpdateCols()
	if len(cols) == 0 {
		t.Fatal("guardedUpdateCols() should not be empty")
	}
}

func TestDecodeTableConfig_InvalidJSON(t *testing.T) {
	if _, err := DecodeTableConfig([]byte("{invalid")); err == nil {
		t.Fatal("expected error")
	}
}
