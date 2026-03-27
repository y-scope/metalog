package metastore

import "testing"

func TestFileState_IsTerminal(t *testing.T) {
	for _, tt := range []struct {
		state FileState
		want  bool
	}{
		{StateIRBuffering, false},
		{StateIRPurging, true},
		{StateArchiveClosed, false},
		{StateArchivePurging, true},
	} {
		if got := tt.state.IsTerminal(); got != tt.want {
			t.Errorf("%q.IsTerminal() = %v, want %v", tt.state, got, tt.want)
		}
	}
}

func TestFileState_CanTransitionTo(t *testing.T) {
	// Representative invalid transitions: self, backward, cross-workflow, skip, from terminal.
	invalid := [][2]FileState{
		{StateIRBuffering, StateIRBuffering},                      // self
		{StateIRClosed, StateIRBuffering},                         // backward
		{StateIRBuffering, StateArchiveClosed},                    // cross-workflow
		{StateIRBuffering, StateIRPurging},                        // skip
		{StateIRPurging, StateIRClosed},                           // from terminal
		{StateIRArchiveBuffering, StateArchiveClosed},             // skip in hybrid
		{StateIRArchiveConsolidationPending, StateArchivePurging}, // skip
	}
	for _, tt := range invalid {
		if tt[0].CanTransitionTo(tt[1]) {
			t.Errorf("CanTransitionTo(%q → %q) = true, want false", tt[0], tt[1])
		}
	}
}

func TestUpsertGuardStates(t *testing.T) {
	got := UpsertGuardStates()
	if len(got) != 4 {
		t.Errorf("len = %d, want 4", len(got))
	}
}
