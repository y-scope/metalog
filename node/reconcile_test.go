package node

import (
	"testing"
)

// TestFairShare validates the ceiling-division fair-share calculation
// used in reconcile() step 2. The formula distributes tables evenly
// across active nodes: fairShare = ceil(totalTables / activeNodes).
func TestFairShare(t *testing.T) {
	tests := []struct {
		name           string
		totalAssigned  int
		unassigned     int
		activeNodes    int
		myTables       int
		wantFairShare  int
		wantCanClaim   int // min(unassigned, fairShare - myTables)
	}{
		{
			name:          "single node claims all",
			totalAssigned: 0, unassigned: 10, activeNodes: 1, myTables: 0,
			wantFairShare: 10, wantCanClaim: 10,
		},
		{
			name:          "two nodes split evenly",
			totalAssigned: 0, unassigned: 10, activeNodes: 2, myTables: 0,
			wantFairShare: 5, wantCanClaim: 5,
		},
		{
			name:          "three nodes with remainder",
			totalAssigned: 0, unassigned: 10, activeNodes: 3, myTables: 0,
			wantFairShare: 4, wantCanClaim: 4, // ceil(10/3) = 4
		},
		{
			name:          "already at fair share",
			totalAssigned: 5, unassigned: 5, activeNodes: 2, myTables: 5,
			wantFairShare: 5, wantCanClaim: 0,
		},
		{
			name:          "over fair share (no claim)",
			totalAssigned: 8, unassigned: 2, activeNodes: 2, myTables: 6,
			wantFairShare: 5, wantCanClaim: 0,
		},
		{
			name:          "under fair share (partial claim)",
			totalAssigned: 3, unassigned: 7, activeNodes: 2, myTables: 3,
			wantFairShare: 5, wantCanClaim: 2,
		},
		{
			name:          "one unassigned one node",
			totalAssigned: 5, unassigned: 1, activeNodes: 1, myTables: 5,
			wantFairShare: 6, wantCanClaim: 1,
		},
		{
			name:          "activeNodes fallback to 1",
			totalAssigned: 0, unassigned: 5, activeNodes: 0, myTables: 0,
			wantFairShare: 5, wantCanClaim: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			activeNodes := tt.activeNodes
			if activeNodes < 1 {
				activeNodes = 1
			}
			totalTables := tt.totalAssigned + tt.unassigned
			fairShare := (totalTables + activeNodes - 1) / activeNodes

			if fairShare != tt.wantFairShare {
				t.Errorf("fairShare = %d, want %d", fairShare, tt.wantFairShare)
			}

			canClaim := fairShare - tt.myTables
			if canClaim < 0 {
				canClaim = 0
			}
			if canClaim > tt.unassigned {
				canClaim = tt.unassigned
			}
			if canClaim != tt.wantCanClaim {
				t.Errorf("canClaim = %d, want %d", canClaim, tt.wantCanClaim)
			}
		})
	}
}

// TestOwnershipVerification validates the set-difference logic used in
// reconcile() step 4 to detect lost and new assignments.
func TestOwnershipVerification(t *testing.T) {
	tests := []struct {
		name        string
		running     []string // coordinators currently running
		assigned    []string // tables assigned in DB
		wantStop    []string // coordinators to stop (running but not assigned)
		wantStart   []string // coordinators to start (assigned but not running)
	}{
		{
			name:      "no changes",
			running:   []string{"a", "b"},
			assigned:  []string{"a", "b"},
			wantStop:  nil,
			wantStart: nil,
		},
		{
			name:      "new assignment",
			running:   []string{"a"},
			assigned:  []string{"a", "b"},
			wantStop:  nil,
			wantStart: []string{"b"},
		},
		{
			name:      "lost assignment",
			running:   []string{"a", "b"},
			assigned:  []string{"a"},
			wantStop:  []string{"b"},
			wantStart: nil,
		},
		{
			name:      "swap",
			running:   []string{"a"},
			assigned:  []string{"b"},
			wantStop:  []string{"a"},
			wantStart: []string{"b"},
		},
		{
			name:      "empty to some",
			running:   nil,
			assigned:  []string{"a", "b"},
			wantStop:  nil,
			wantStart: []string{"a", "b"},
		},
		{
			name:      "some to empty",
			running:   []string{"a", "b"},
			assigned:  nil,
			wantStop:  []string{"a", "b"},
			wantStart: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runningSet := make(map[string]bool, len(tt.running))
			for _, r := range tt.running {
				runningSet[r] = true
			}
			assignedSet := make(map[string]bool, len(tt.assigned))
			for _, a := range tt.assigned {
				assignedSet[a] = true
			}

			// Stop: running but not assigned
			var toStop []string
			for _, r := range tt.running {
				if !assignedSet[r] {
					toStop = append(toStop, r)
				}
			}

			// Start: assigned but not running (after stops are processed)
			for _, r := range toStop {
				delete(runningSet, r)
			}
			var toStart []string
			for _, a := range tt.assigned {
				if !runningSet[a] {
					toStart = append(toStart, a)
				}
			}

			if !stringSliceEqual(toStop, tt.wantStop) {
				t.Errorf("toStop = %v, want %v", toStop, tt.wantStop)
			}
			if !stringSliceEqual(toStart, tt.wantStart) {
				t.Errorf("toStart = %v, want %v", toStart, tt.wantStart)
			}
		})
	}
}

func stringSliceEqual(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
