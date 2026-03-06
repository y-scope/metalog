//go:build integration

package metastore_test

import (
	"context"
	"database/sql"
	"testing"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/testutil"
)

const testTable = "test_logs"

func setupFileRecordsIT(t *testing.T) (*testutil.MariaDBContainer, *metastore.FileRecords) {
	t.Helper()
	mc := testutil.SetupMariaDB(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, testTable)

	log := zap.NewNop()
	fr, err := metastore.NewFileRecords(mc.DB, testTable, log)
	if err != nil {
		mc.Teardown(t)
		t.Fatal(err)
	}
	return mc, fr
}

func TestFileRecords_UpsertBatch_InsertAndQuery(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	records := []*metastore.FileRecord{
		{
			MinTimestamp:        1704067200000000000, // 2024-01-01
			MaxTimestamp:        1704067200100000000,
			ClpIRStorageBackend: sql.NullString{String: "minio", Valid: true},
			ClpIRBucket:         sql.NullString{String: "logs", Valid: true},
			ClpIRPath:           sql.NullString{String: "/data/file1.ir", Valid: true},
			State:               metastore.StateIRBuffering,
			RecordCount:         100,
			RetentionDays:       30,
		},
		{
			MinTimestamp:        1704067200000000000,
			MaxTimestamp:        1704067200200000000,
			ClpIRStorageBackend: sql.NullString{String: "minio", Valid: true},
			ClpIRBucket:         sql.NullString{String: "logs", Valid: true},
			ClpIRPath:           sql.NullString{String: "/data/file2.ir", Valid: true},
			State:               metastore.StateIRBuffering,
			RecordCount:         200,
			RetentionDays:       30,
		},
	}

	affected, err := fr.UpsertBatch(ctx, records, nil, nil, nil)
	if err != nil {
		t.Fatalf("UpsertBatch() error = %v", err)
	}
	if affected < 2 {
		t.Errorf("UpsertBatch() affected = %d, want >= 2", affected)
	}

	// Verify rows exist
	var count int
	err = mc.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `"+testTable+"`").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("row count = %d, want 2", count)
	}
}

func TestFileRecords_UpsertBatch_GuardedUpdate(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// Insert initial record
	records := []*metastore.FileRecord{
		{
			MinTimestamp:        1704067200000000000,
			MaxTimestamp:        1704067200100000000,
			ClpIRStorageBackend: sql.NullString{String: "minio", Valid: true},
			ClpIRBucket:         sql.NullString{String: "logs", Valid: true},
			ClpIRPath:           sql.NullString{String: "/data/guarded.ir", Valid: true},
			State:               metastore.StateIRBuffering,
			RecordCount:         100,
			RetentionDays:       30,
		},
	}
	_, err := fr.UpsertBatch(ctx, records, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Upsert with higher max_timestamp — should update
	records[0].MaxTimestamp = 1704067200200000000
	records[0].RecordCount = 200
	_, err = fr.UpsertBatch(ctx, records, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	var maxTs int64
	var recordCount uint32
	err = mc.DB.QueryRowContext(ctx,
		"SELECT max_timestamp, record_count FROM `"+testTable+
			"` WHERE clp_ir_path_hash = UNHEX(MD5(?))", "/data/guarded.ir").
		Scan(&maxTs, &recordCount)
	if err != nil {
		t.Fatal(err)
	}
	if maxTs != 1704067200200000000 {
		t.Errorf("max_timestamp = %d, want 1704067200200000000", maxTs)
	}
	if recordCount != 200 {
		t.Errorf("record_count = %d, want 200", recordCount)
	}
}

func TestFileRecords_UpsertBatch_GuardPreventsOverwrite(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// Insert record in ARCHIVE_CLOSED state (guarded)
	_, err := mc.DB.ExecContext(ctx,
		"INSERT INTO `"+testTable+"` (min_timestamp, max_timestamp, clp_ir_path, state, record_count, retention_days) "+
			"VALUES (?, ?, ?, ?, ?, ?)",
		1704067200000000000, 1704067200100000000,
		"/data/protected.ir", "ARCHIVE_CLOSED", 100, 30)
	if err != nil {
		t.Fatal(err)
	}

	// Try to overwrite with UPSERT — guard should prevent it
	records := []*metastore.FileRecord{
		{
			MinTimestamp:  1704067200000000000,
			MaxTimestamp:  1704067200999000000, // higher
			ClpIRPath:     sql.NullString{String: "/data/protected.ir", Valid: true},
			State:         metastore.StateIRBuffering, // trying to regress state
			RecordCount:   999,
			RetentionDays: 30,
		},
	}
	_, err = fr.UpsertBatch(ctx, records, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// State should still be ARCHIVE_CLOSED
	var state string
	var recordCount uint32
	err = mc.DB.QueryRowContext(ctx,
		"SELECT state, record_count FROM `"+testTable+
			"` WHERE clp_ir_path_hash = UNHEX(MD5(?))", "/data/protected.ir").
		Scan(&state, &recordCount)
	if err != nil {
		t.Fatal(err)
	}
	if state != "ARCHIVE_CLOSED" {
		t.Errorf("state = %q, want ARCHIVE_CLOSED (guard should prevent overwrite)", state)
	}
	if recordCount != 100 {
		t.Errorf("record_count = %d, want 100 (guard should prevent overwrite)", recordCount)
	}
}

func TestFileRecords_FindConsolidationPending(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// Insert records in different states
	states := []string{
		"IR_ARCHIVE_CONSOLIDATION_PENDING",
		"IR_ARCHIVE_CONSOLIDATION_PENDING",
		"IR_BUFFERING",
		"ARCHIVE_CLOSED",
	}
	for i, s := range states {
		_, err := mc.DB.ExecContext(ctx,
			"INSERT INTO `"+testTable+"` (min_timestamp, max_timestamp, clp_ir_path, state, record_count, retention_days) VALUES (?, ?, ?, ?, ?, ?)",
			1704067200000000000, 1704067200100000000+int64(i),
			"/data/consolidation_"+string(rune('a'+i))+".ir", s, 10, 30)
		if err != nil {
			t.Fatalf("insert state=%s: %v", s, err)
		}
	}

	pending, err := fr.FindConsolidationPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Errorf("FindConsolidationPending() returned %d records, want 2", len(pending))
	}
}

func TestFileRecords_GetCurrentStates(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	irPaths := []string{"/data/state1.ir", "/data/state2.ir"}
	for i, p := range irPaths {
		_, err := mc.DB.ExecContext(ctx,
			"INSERT INTO `"+testTable+"` (min_timestamp, max_timestamp, clp_ir_path, state, record_count, retention_days) VALUES (?, ?, ?, ?, ?, ?)",
			1704067200000000000, 1704067200100000000+int64(i),
			p, "IR_BUFFERING", 10, 30)
		if err != nil {
			t.Fatal(err)
		}
	}

	states, err := fr.GetCurrentStates(ctx, irPaths)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 {
		t.Errorf("GetCurrentStates() returned %d entries, want 2", len(states))
	}
	for _, p := range irPaths {
		if states[p] != metastore.StateIRBuffering {
			t.Errorf("state for %s = %q, want IR_BUFFERING", p, states[p])
		}
	}
}

func TestFileRecords_UpdateState(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// Insert in IR_BUFFERING
	irPath := "/data/transition.ir"
	_, err := mc.DB.ExecContext(ctx,
		"INSERT INTO `"+testTable+"` (min_timestamp, max_timestamp, clp_ir_path, state, record_count, retention_days) VALUES (?, ?, ?, ?, ?, ?)",
		1704067200000000000, 1704067200100000000, irPath, "IR_BUFFERING", 10, 30)
	if err != nil {
		t.Fatal(err)
	}

	// Transition to IR_CLOSED
	affected, err := fr.UpdateState(ctx, []string{irPath}, metastore.StateIRClosed)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Errorf("UpdateState() affected = %d, want 1", affected)
	}

	// Verify
	states, _ := fr.GetCurrentStates(ctx, []string{irPath})
	if states[irPath] != metastore.StateIRClosed {
		t.Errorf("state = %q, want IR_CLOSED", states[irPath])
	}
}

func TestFileRecords_UpdateState_InvalidTransition(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// Insert in IR_BUFFERING
	irPath := "/data/invalid_transition.ir"
	_, err := mc.DB.ExecContext(ctx,
		"INSERT INTO `"+testTable+"` (min_timestamp, max_timestamp, clp_ir_path, state, record_count, retention_days) VALUES (?, ?, ?, ?, ?, ?)",
		1704067200000000000, 1704067200100000000, irPath, "IR_BUFFERING", 10, 30)
	if err != nil {
		t.Fatal(err)
	}

	// Try invalid transition: IR_BUFFERING -> ARCHIVE_CLOSED
	_, err = fr.UpdateState(ctx, []string{irPath}, metastore.StateArchiveClosed)
	if err == nil {
		t.Error("UpdateState() with invalid transition should return error")
	}
}
