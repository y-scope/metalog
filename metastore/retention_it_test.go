package metastore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
)

func TestFileRecords_TransitionExpiredToPurging_IROnly(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	now := time.Now().UnixNano()
	pastExpiry := now - int64(time.Hour)
	futureExpiry := now + int64(time.Hour)

	// Insert IR_CLOSED files: 2 expired, 1 not expired.
	for i, exp := range []int64{pastExpiry, pastExpiry, futureExpiry} {
		insertTestRecord(t, mc.DB,
			1704067200000000000, 1704067200100000000+int64(i),
			fmt.Sprintf("/data/ir_retention_%d.ir", i), "IR_CLOSED", exp)
	}

	transitioned, err := fr.TransitionExpiredToPurging(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if transitioned != 2 {
		t.Errorf("TransitionExpiredToPurging() = %d, want 2", transitioned)
	}

	// Verify states.
	var purging, closed int
	rows, err := mc.DB.QueryContext(ctx, "SELECT state FROM `"+testTable+"`")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var state string
		if err := rows.Scan(&state); err != nil {
			t.Fatal(err)
		}
		switch state {
		case "IR_PURGING":
			purging++
		case "IR_CLOSED":
			closed++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if purging != 2 {
		t.Errorf("IR_PURGING count = %d, want 2", purging)
	}
	if closed != 1 {
		t.Errorf("IR_CLOSED count = %d, want 1", closed)
	}
}

func TestFileRecords_TransitionExpiredToPurging_ArchiveOnly(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	now := time.Now().UnixNano()
	pastExpiry := now - int64(time.Hour)

	// Insert ARCHIVE_CLOSED file with past expiry.
	insertTestRecord(t, mc.DB,
		1704067200000000000, 1704067200100000000,
		"/data/archive_retention.ir", "ARCHIVE_CLOSED", pastExpiry)

	transitioned, err := fr.TransitionExpiredToPurging(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if transitioned != 1 {
		t.Errorf("TransitionExpiredToPurging() = %d, want 1", transitioned)
	}

	var state string
	err = mc.DB.QueryRowContext(ctx,
		"SELECT state FROM `"+testTable+"` WHERE clp_ir_path_hash = UNHEX(MD5(?))",
		"/data/archive_retention.ir").Scan(&state)
	if err != nil {
		t.Fatal(err)
	}
	if state != "ARCHIVE_PURGING" {
		t.Errorf("state = %q, want ARCHIVE_PURGING", state)
	}
}

func TestFileRecords_TransitionExpiredToPurging_SkipsWrongStates(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	now := time.Now().UnixNano()
	pastExpiry := now - int64(time.Hour)

	// Insert files in states that should NOT be transitioned.
	states := []string{"IR_BUFFERING", "IR_ARCHIVE_BUFFERING", "IR_ARCHIVE_CONSOLIDATION_PENDING"}
	for i, s := range states {
		insertTestRecord(t, mc.DB,
			1704067200000000000, 1704067200100000000+int64(i),
			fmt.Sprintf("/data/skip_%d.ir", i), s, pastExpiry)
	}

	transitioned, err := fr.TransitionExpiredToPurging(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if transitioned != 0 {
		t.Errorf("TransitionExpiredToPurging() = %d, want 0 (wrong states should be skipped)", transitioned)
	}
}

func TestFileRecords_TransitionExpiredToPurging_ZeroExpiresAtIgnored(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	now := time.Now().UnixNano()

	// Insert IR_CLOSED file with expires_at = 0 (no expiration set).
	insertTestRecord(t, mc.DB,
		1704067200000000000, 1704067200100000000,
		"/data/no_expiry.ir", "IR_CLOSED", 0)

	transitioned, err := fr.TransitionExpiredToPurging(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if transitioned != 0 {
		t.Errorf("TransitionExpiredToPurging() = %d, want 0 (expires_at=0 should be ignored)", transitioned)
	}
}

func TestFileRecords_FullRetentionPipeline(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	now := time.Now().UnixNano()
	pastExpiry := now - int64(time.Hour)

	// Insert IR_CLOSED file with storage paths and past expiry.
	query, args, _ := sq.Insert("`"+testTable+"`").
		Columns("min_timestamp", "max_timestamp",
			"clp_ir_storage_backend", "clp_ir_bucket", "clp_ir_path",
			"state", "record_count", "retention_days", "expires_at").
		Values(1704067200000000000, 1704067200100000000,
			"s3", "test-bucket", "/data/pipeline.ir",
			"IR_CLOSED", 10, 30, pastExpiry).
		ToSql()
	if _, err := mc.DB.ExecContext(ctx, query, args...); err != nil {
		t.Fatal(err)
	}

	// Phase 1: transition to purging.
	transitioned, err := fr.TransitionExpiredToPurging(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if transitioned != 1 {
		t.Fatalf("phase 1: transitioned = %d, want 1", transitioned)
	}

	// Phase 2: delete expired files (now in PURGING state).
	result, err := fr.DeleteExpiredFiles(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedCount != 1 {
		t.Errorf("phase 2: deleted = %d, want 1", result.DeletedCount)
	}
	if len(result.IRPaths) != 1 {
		t.Fatalf("phase 2: IR paths = %d, want 1", len(result.IRPaths))
	}
	if result.IRPaths[0].Backend != "s3" || result.IRPaths[0].Bucket != "test-bucket" || result.IRPaths[0].Path != "/data/pipeline.ir" {
		t.Errorf("IR path = %+v, want s3/test-bucket//data/pipeline.ir", result.IRPaths[0])
	}

	// Verify row is gone.
	var count int
	err = mc.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+testTable+"`").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("remaining rows = %d, want 0", count)
	}
}

func TestFileRecords_DeleteExpiredFiles_TOCTOUProtection(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	now := time.Now().UnixNano()
	pastExpiry := now - int64(time.Hour)

	// Insert file already in IR_PURGING with past expiry.
	insertTestRecord(t, mc.DB,
		1704067200000000000, 1704067200100000000,
		"/data/toctou.ir", "IR_PURGING", pastExpiry)

	// Simulate retention extension (another tx extended expires_at after SELECT).
	futureExpiry := now + int64(24*time.Hour)
	_, err := mc.DB.ExecContext(ctx,
		"UPDATE `"+testTable+"` SET expires_at = ? WHERE clp_ir_path_hash = UNHEX(MD5(?))",
		futureExpiry, "/data/toctou.ir")
	if err != nil {
		t.Fatal(err)
	}

	// DeleteExpiredFiles should not delete — expires_at was extended.
	result, err := fr.DeleteExpiredFiles(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedCount != 0 {
		t.Errorf("DeletedCount = %d, want 0 (TOCTOU: expires_at was extended)", result.DeletedCount)
	}

	// Verify row still exists.
	var count int
	err = mc.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+testTable+"`").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1 (file should survive)", count)
	}
}

func TestFileRecords_DeleteExpiredFiles_ArchiveWithBothPaths(t *testing.T) {
	mc, fr := setupFileRecordsIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	now := time.Now().UnixNano()
	pastExpiry := now - int64(time.Hour)

	// Insert ARCHIVE_PURGING file that has both IR and archive paths
	// (hybrid lifecycle — consolidated but IR path still recorded).
	query, args, _ := sq.Insert("`"+testTable+"`").
		Columns("min_timestamp", "max_timestamp",
			"clp_ir_storage_backend", "clp_ir_bucket", "clp_ir_path",
			"clp_archive_storage_backend", "clp_archive_bucket", "clp_archive_path",
			"state", "record_count", "retention_days", "expires_at").
		Values(1704067200000000000, 1704067200100000000,
			"s3", "ir-bucket", "/data/hybrid.ir",
			"s3", "archive-bucket", "/archives/hybrid.clp",
			"ARCHIVE_PURGING", 10, 30, pastExpiry).
		ToSql()
	if _, err := mc.DB.ExecContext(ctx, query, args...); err != nil {
		t.Fatal(err)
	}

	result, err := fr.DeleteExpiredFiles(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedCount != 1 {
		t.Errorf("DeletedCount = %d, want 1", result.DeletedCount)
	}
	if len(result.IRPaths) != 1 {
		t.Errorf("IR paths = %d, want 1", len(result.IRPaths))
	}
	if len(result.ArchivePaths) != 1 {
		t.Errorf("Archive paths = %d, want 1", len(result.ArchivePaths))
	}
	if result.ArchivePaths[0].Path != "/archives/hybrid.clp" {
		t.Errorf("archive path = %q, want /archives/hybrid.clp", result.ArchivePaths[0].Path)
	}
}
