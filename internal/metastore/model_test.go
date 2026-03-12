package metastore

import (
	"testing"
)

func TestFileState_IsTerminal(t *testing.T) {
	tests := []struct {
		state    FileState
		terminal bool
	}{
		{StateIRBuffering, false},
		{StateIRClosed, false},
		{StateIRPurging, true},
		{StateArchiveClosed, false},
		{StateArchivePurging, true},
		{StateIRArchiveBuffering, false},
		{StateIRArchiveConsolidationPending, false},
	}
	for _, tt := range tests {
		if got := tt.state.IsTerminal(); got != tt.terminal {
			t.Errorf("FileState(%q).IsTerminal() = %v, want %v", tt.state, got, tt.terminal)
		}
	}
}

func TestFileState_CanTransitionTo_InvalidTransitions(t *testing.T) {
	// Invalid transitions should return false
	invalidTransitions := []struct {
		from FileState
		to   FileState
	}{
		// Self-transitions are invalid
		{StateIRBuffering, StateIRBuffering},
		{StateIRClosed, StateIRClosed},
		{StateIRPurging, StateIRPurging},
		{StateArchiveClosed, StateArchiveClosed},
		{StateArchivePurging, StateArchivePurging},
		// Cross-workflow transitions
		{StateIRBuffering, StateArchiveClosed},
		{StateIRBuffering, StateArchivePurging},
		{StateIRBuffering, StateIRArchiveConsolidationPending},
		{StateIRClosed, StateArchiveClosed},
		{StateIRClosed, StateArchivePurging},
		// Backwards transitions
		{StateIRClosed, StateIRBuffering},
		{StateIRPurging, StateIRClosed},
		{StateArchivePurging, StateArchiveClosed},
		{StateArchiveClosed, StateIRArchiveConsolidationPending},
		// From terminal states
		{StateIRPurging, StateIRBuffering},
		{StateIRPurging, StateArchiveClosed},
		{StateArchivePurging, StateIRBuffering},
		{StateArchivePurging, StateIRClosed},
		// Skip states
		{StateIRBuffering, StateIRPurging},
		{StateIRArchiveBuffering, StateArchiveClosed},
		{StateIRArchiveBuffering, StateArchivePurging},
		{StateIRArchiveConsolidationPending, StateArchivePurging},
	}
	for _, tt := range invalidTransitions {
		if tt.from.CanTransitionTo(tt.to) {
			t.Errorf("CanTransitionTo(%q -> %q) = true, want false", tt.from, tt.to)
		}
	}
}

func TestUpsertGuardStates(t *testing.T) {
	// Verify the guard states are correctly defined
	expected := map[FileState]bool{
		StateIRPurging:                     true,
		StateIRArchiveConsolidationPending: true,
		StateArchiveClosed:                 true,
		StateArchivePurging:                true,
	}
	if len(UpsertGuardStates) != len(expected) {
		t.Errorf("len(UpsertGuardStates) = %d, want %d", len(UpsertGuardStates), len(expected))
	}
	for _, state := range UpsertGuardStates {
		if !expected[state] {
			t.Errorf("unexpected state in UpsertGuardStates: %q", state)
		}
	}
}
