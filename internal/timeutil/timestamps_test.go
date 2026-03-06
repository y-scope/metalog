package timeutil

import (
	"testing"
	"time"
)

func TestEpochNanos(t *testing.T) {
	before := time.Now().UnixNano()
	got := EpochNanos()
	after := time.Now().UnixNano()
	if got < before || got > after {
		t.Errorf("EpochNanos() = %d, want between %d and %d", got, before, after)
	}
}

func TestNanosToTimeRoundTrip(t *testing.T) {
	nanos := int64(1704067200000000000) // 2024-01-01T00:00:00Z
	got := TimeToNanos(NanosToTime(nanos))
	if got != nanos {
		t.Errorf("round trip: got %d, want %d", got, nanos)
	}
}

func TestDayBoundaryNanos(t *testing.T) {
	// 2024-01-15T14:30:00Z
	nanos := int64(1705327800000000000)
	got := DayBoundaryNanos(nanos)
	// Should be 2024-01-15T00:00:00Z
	want := int64(1705276800000000000)
	if got != want {
		t.Errorf("DayBoundaryNanos() = %d, want %d", got, want)
	}
}

func TestDayPartitionName(t *testing.T) {
	nanos := int64(1704067200000000000) // 2024-01-01T00:00:00Z
	got := DayPartitionName(nanos)
	if got != "p_20240101" {
		t.Errorf("DayPartitionName() = %q, want %q", got, "p_20240101")
	}
}

func TestAddDaysNanos(t *testing.T) {
	nanos := int64(1704067200000000000) // 2024-01-01T00:00:00Z
	got := AddDaysNanos(nanos, 7)
	want := int64(1704672000000000000) // 2024-01-08T00:00:00Z
	if got != want {
		t.Errorf("AddDaysNanos() = %d, want %d", got, want)
	}
}
