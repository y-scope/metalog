package schema

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/logutil"
)

// testCR creates a fully initialized ColumnRegistry for allocation tests.
func testCR() *ColumnRegistry {
	cr := newTestRegistry()
	cr.exhaustionFL = logutil.NewFailureLogger(zap.NewNop(), time.Minute)
	cr.initMetrics(noop.Meter{})
	return cr
}

// testCRWithDB creates a fully initialized ColumnRegistry backed by sqlmock.
func testCRWithDB(db *sql.DB) *ColumnRegistry {
	cr := newTestRegistryWithDB(db)
	cr.exhaustionFL = logutil.NewFailureLogger(zap.NewNop(), time.Minute)
	cr.initMetrics(noop.Meter{})
	return cr
}

// --- lookupDimFromDB ---

func TestLookupDimFromDB_Error(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	mock.ExpectQuery("SELECT column_name FROM _dim_registry").
		WillReturnError(fmt.Errorf("db error"))

	_, err := cr.lookupDimFromDB(context.Background(), "host")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestLookupAggFromDB_EmptyAggValue(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	mock.ExpectQuery("SELECT column_name FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("agg_f02"))

	col, err := cr.lookupAggFromDB(context.Background(), "count", "", "SUM")
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if col != "agg_f02" {
		t.Errorf("col = %q, want agg_f02", col)
	}
}

// --- expandDimWidth ---

func TestExpandDimWidth_AlreadyWide(t *testing.T) {
	db, _, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true
	entry := &DimRegistryEntry{
		TableName: "test_table", ColumnName: "dim_f01",
		BaseType: "str", Width: 500, DimKey: "host", Status: statusActive,
	}
	cr.dimByKey["host"] = entry
	cr.dimByColumn["dim_f01"] = entry

	// newWidth <= entry.Width → should return immediately
	col, err := cr.expandDimWidth(context.Background(), entry, 300)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if col != "dim_f01" {
		t.Errorf("col = %q, want dim_f01", col)
	}
}

func TestExpandDimWidth_AlterFails(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true
	entry := &DimRegistryEntry{
		TableName: "test_table", ColumnName: "dim_f01",
		BaseType: "str", Width: 256, DimKey: "host", Status: statusActive,
	}
	cr.dimByKey["host"] = entry
	cr.dimByColumn["dim_f01"] = entry

	mock.ExpectExec("ALTER TABLE").WillReturnError(fmt.Errorf("alter failed"))

	_, err := cr.expandDimWidth(context.Background(), entry, 500)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestExpandDimWidth_ConcurrentInvalidation(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true
	entry := &DimRegistryEntry{
		TableName: "test_table", ColumnName: "dim_f01",
		BaseType: "str", Width: 256, DimKey: "host", Status: statusActive,
	}
	cr.dimByKey["host"] = entry
	cr.dimByColumn["dim_f01"] = entry

	// ALTER succeeds
	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	// UPDATE returns 0 rows (concurrently invalidated)
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))
	// Fix query for invalidated column
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))

	_, err := cr.expandDimWidth(context.Background(), entry, 500)
	if err == nil {
		t.Fatal("expected error for concurrent invalidation")
	}
}

// --- ResolveOrAllocateDim ---

func TestResolveOrAllocateDim_NeedsWidening(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true
	entry := &DimRegistryEntry{
		TableName: "test_table", ColumnName: "dim_f01",
		BaseType: "str", Width: 256, DimKey: "host", Status: statusActive,
	}
	cr.dimByKey["host"] = entry
	cr.dimByColumn["dim_f01"] = entry

	// ALTER TABLE MODIFY
	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	// UPDATE registry
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))

	col, err := cr.ResolveOrAllocateDim(context.Background(), "host", "str", 500)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if col != "dim_f01" {
		t.Errorf("col = %q", col)
	}
}

func TestResolveOrAllocateDim_NoWideningForNonStr(t *testing.T) {
	cr := testCR()
	entry := &DimRegistryEntry{
		ColumnName: "dim_f01", DimKey: "count", BaseType: "int", Width: 0,
	}
	cr.dimByKey["count"] = entry
	cr.dimByColumn["dim_f01"] = entry

	// Width > e.Width but baseType is "int" → no widening
	col, err := cr.ResolveOrAllocateDim(context.Background(), "count", "int", 500)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if col != "dim_f01" {
		t.Errorf("col = %q", col)
	}
}

// --- ResolveOrAllocateDims (batch) ---

func TestResolveOrAllocateDims_WithWidening(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true
	entry := &DimRegistryEntry{
		TableName: "test_table", ColumnName: "dim_f01",
		BaseType: "str", Width: 256, DimKey: "host", Status: statusActive,
	}
	cr.dimByKey["host"] = entry
	cr.dimByColumn["dim_f01"] = entry

	// ALTER TABLE MODIFY for widening
	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	// UPDATE registry width
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))

	result, err := cr.ResolveOrAllocateDims(context.Background(), []DimRequest{
		{DimKey: "host", BaseType: "str", Width: 500},
	})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if result["host"] != "dim_f01" {
		t.Errorf("host = %q", result["host"])
	}
}

// --- ResolveOrAllocateAggs (batch) ---

// --- batchAllocateDimSlots ---

func TestBatchAllocateDimSlots_SlotsExhausted(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true
	cr.nextDimSlot = 100 // all 99 slots used

	// Advisory lock
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(
		sqlmock.NewRows([]string{"result"}).AddRow(1))

	// lookupDimFromDB — not found
	mock.ExpectQuery("SELECT column_name FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))

	// claimAvailableDimSlot: no available
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, base_type, width FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "base_type", "width"}))
	mock.ExpectRollback()

	// RELEASE_LOCK
	mock.ExpectExec("SELECT RELEASE_LOCK").WillReturnResult(sqlmock.NewResult(0, 0))

	result, err := cr.batchAllocateDimSlots(context.Background(), []DimRequest{
		{DimKey: "new_field", BaseType: "str", Width: 256},
	})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	// Should be empty — slots exhausted
	if _, ok := result["new_field"]; ok {
		t.Error("expected no allocation when slots exhausted")
	}
}

func TestBatchAllocateDimSlots_PartialExhaustion(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true
	cr.nextDimSlot = 99 // only 1 slot left

	// Advisory lock
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(
		sqlmock.NewRows([]string{"result"}).AddRow(1))

	// lookupDimFromDB — not found (for both requests)
	mock.ExpectQuery("SELECT column_name FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	mock.ExpectQuery("SELECT column_name FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))

	// claimAvailableDimSlot for field1 — no available
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, base_type, width FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "base_type", "width"}))
	mock.ExpectRollback()

	// claimAvailableDimSlot for field2 — no available
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, base_type, width FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "base_type", "width"}))
	mock.ExpectRollback()

	// ALTER TABLE for 1 column (remaining=1, so only first gets allocated)
	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))

	// INSERT for 1 column
	mock.ExpectExec("INSERT INTO _dim_registry").WillReturnResult(sqlmock.NewResult(1, 1))

	// RELEASE_LOCK
	mock.ExpectExec("SELECT RELEASE_LOCK").WillReturnResult(sqlmock.NewResult(0, 0))

	result, err := cr.batchAllocateDimSlots(context.Background(), []DimRequest{
		{DimKey: "field1", BaseType: "str", Width: 256},
		{DimKey: "field2", BaseType: "str", Width: 256},
	})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	// Only field1 should be allocated
	if _, ok := result["field1"]; !ok {
		t.Error("field1 should be allocated")
	}
	if _, ok := result["field2"]; ok {
		t.Error("field2 should NOT be allocated (exhausted)")
	}
}

// --- batchAllocateAggSlots ---

func TestBatchAllocateAggSlots_FloatType(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true
	cr.nextAggSlot = 1

	// Advisory lock
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(
		sqlmock.NewRows([]string{"result"}).AddRow(1))

	// lookupAggFromDB — not found
	mock.ExpectQuery("SELECT column_name FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))

	// claimAvailableAggSlot: no available
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, value_type FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "value_type"}))
	mock.ExpectRollback()

	// ALTER TABLE ADD COLUMN (DOUBLE for FLOAT)
	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))

	// INSERT
	mock.ExpectExec("INSERT INTO _agg_registry").WillReturnResult(sqlmock.NewResult(1, 1))

	// RELEASE_LOCK
	mock.ExpectExec("SELECT RELEASE_LOCK").WillReturnResult(sqlmock.NewResult(0, 0))

	result, err := cr.batchAllocateAggSlots(context.Background(), []AggRequest{
		{AggKey: "avg_duration", AggType: "AVG", ValueType: "FLOAT"},
	})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	key := AggCacheKey("avg_duration", "", "AVG")
	if result[key] != "agg_f01" {
		t.Errorf("result = %q, want agg_f01", result[key])
	}
}

func TestBatchAllocateAggSlots_SlotsExhausted(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true
	cr.nextAggSlot = 100

	// Advisory lock
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(
		sqlmock.NewRows([]string{"result"}).AddRow(1))

	// lookupAggFromDB — not found
	mock.ExpectQuery("SELECT column_name FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))

	// claimAvailableAggSlot: no available
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, value_type FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "value_type"}))
	mock.ExpectRollback()

	// RELEASE_LOCK
	mock.ExpectExec("SELECT RELEASE_LOCK").WillReturnResult(sqlmock.NewResult(0, 0))

	result, err := cr.batchAllocateAggSlots(context.Background(), []AggRequest{
		{AggKey: "new_agg", AggType: "SUM", ValueType: "INT"},
	})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	key := AggCacheKey("new_agg", "", "SUM")
	if _, ok := result[key]; ok {
		t.Error("should not allocate when slots exhausted")
	}
}

// --- claimAvailableDimSlot ---

func TestClaimAvailableDimSlot_NoneAvailable(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, base_type, width FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "base_type", "width"}))
	mock.ExpectRollback()

	col, err := cr.claimAvailableDimSlot(context.Background(), "host", "str", 256)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if col != "" {
		t.Errorf("expected empty, got %q", col)
	}
}

func TestClaimAvailableDimSlot_DifferentType(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true

	// Begin tx
	mock.ExpectBegin()
	// SELECT FOR UPDATE SKIP LOCKED — find available slot with different type
	mock.ExpectQuery("SELECT column_name, base_type, width FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "base_type", "width"}).
			AddRow("dim_f03", "int", sql.NullInt32{Valid: false}))
	// UPDATE to claim
	mock.ExpectExec("UPDATE _dim_registry").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// ALTER TABLE MODIFY COLUMN (type changed from INT to VARCHAR)
	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))

	col, err := cr.claimAvailableDimSlot(context.Background(), "host", "str", 256)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if col != "dim_f03" {
		t.Errorf("col = %q, want dim_f03", col)
	}
}

func TestClaimAvailableDimSlot_AlterFails_Revert(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true

	// Begin tx
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, base_type, width FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "base_type", "width"}).
			AddRow("dim_f03", "int", sql.NullInt32{Valid: false}))
	mock.ExpectExec("UPDATE _dim_registry").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// ALTER fails
	mock.ExpectExec("ALTER TABLE").WillReturnError(fmt.Errorf("alter failed"))

	// Revert: mark slot back to AVAILABLE
	mock.ExpectExec("UPDATE _dim_registry").WillReturnResult(sqlmock.NewResult(0, 1))

	_, err := cr.claimAvailableDimSlot(context.Background(), "host", "str", 256)
	if err == nil {
		t.Fatal("expected error when ALTER fails")
	}
}

// --- claimAvailableAggSlot ---

func TestClaimAvailableAggSlot_TypeChange(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, value_type FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "value_type"}).
			AddRow("agg_f05", "INT"))
	mock.ExpectExec("UPDATE _agg_registry").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// ALTER TABLE MODIFY (INT → DOUBLE)
	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))

	col, err := cr.claimAvailableAggSlot(context.Background(), "avg", "", "AVG", "FLOAT")
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if col != "agg_f05" {
		t.Errorf("col = %q, want agg_f05", col)
	}
}

// --- ResolveOrAllocateSketches ---

func TestResolveOrAllocateSketches_ClaimSlot(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	// claimSketchSlot: begin tx
	mock.ExpectBegin()
	// UPDATE to claim
	mock.ExpectExec("UPDATE _sketch_registry").WillReturnResult(sqlmock.NewResult(0, 1))
	// Read back claimed slot name
	mock.ExpectQuery("SELECT sketch_name FROM _sketch_registry").
		WillReturnRows(sqlmock.NewRows([]string{"sketch_name"}).AddRow("s03"))
	mock.ExpectCommit()

	result, err := cr.ResolveOrAllocateSketches(context.Background(), []string{"uuid"})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if result["uuid"] != "s03" {
		t.Errorf("uuid = %q, want s03", result["uuid"])
	}
}

func TestResolveOrAllocateSketches_SlotsExhausted(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	// claimSketchSlot: begin tx
	mock.ExpectBegin()
	// UPDATE claims 0 rows (all slots used)
	mock.ExpectExec("UPDATE _sketch_registry").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	_, err := cr.ResolveOrAllocateSketches(context.Background(), []string{"uuid"})
	if err == nil {
		t.Fatal("expected error when all sketch slots exhausted")
	}
}

// --- revertClaimedSlot ---

func TestRevertClaimedSlot_Dim(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	mock.ExpectExec("UPDATE _dim_registry").WillReturnResult(sqlmock.NewResult(0, 1))

	cr.revertClaimedSlot(context.Background(), "_dim_registry", "dim_f03")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestRevertClaimedSlot_Agg(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	mock.ExpectExec("UPDATE _agg_registry").WillReturnResult(sqlmock.NewResult(0, 1))

	cr.revertClaimedSlot(context.Background(), "_agg_registry", "agg_f05")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// --- allocateNewDimSlot (single) ---

func TestAllocateNewDimSlot_DoubleCheck(t *testing.T) {
	// When another goroutine allocated while we waited for allocMu
	cr := testCR()
	cr.dimByKey["host"] = &DimRegistryEntry{ColumnName: "dim_f01", DimKey: "host"}

	col, err := cr.allocateNewDimSlot(context.Background(), "host", "str", 256)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if col != "dim_f01" {
		t.Errorf("col = %q, want dim_f01", col)
	}
}

func TestAllocateNewDimSlot_SlotsExhausted(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.nextDimSlot = 100

	// Advisory lock
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(
		sqlmock.NewRows([]string{"result"}).AddRow(1))

	// lookupDimFromDB — not found
	mock.ExpectQuery("SELECT column_name FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))

	// claimAvailableDimSlot — no available
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, base_type, width FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "base_type", "width"}))
	mock.ExpectRollback()

	// RELEASE_LOCK
	mock.ExpectExec("SELECT RELEASE_LOCK").WillReturnResult(sqlmock.NewResult(0, 0))

	_, err := cr.allocateNewDimSlot(context.Background(), "newfield", "str", 256)
	if err != ErrSlotExhausted {
		t.Fatalf("expected ErrSlotExhausted, got %v", err)
	}
}

// --- allocateNewAggSlot (single) ---

// --- claimAvailableDimSlotTx edge cases ---

func TestClaimAvailableDimSlotTx_ZeroRowsAffected(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, base_type, width FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "base_type", "width"}).
			AddRow("dim_f03", "str", sql.NullInt32{Valid: true, Int32: 256}))
	// UPDATE returns 0 rows (lost CAS race)
	mock.ExpectExec("UPDATE _dim_registry").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	col, _, _, err := cr.claimAvailableDimSlotTx(context.Background(), "host", "str", 256)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if col != "" {
		t.Errorf("expected empty on zero rows affected, got %q", col)
	}
}

// --- claimSketchSlot ---

func TestClaimSketchSlot_BeginTxError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	mock.ExpectBegin().WillReturnError(fmt.Errorf("connection failed"))

	_, err := cr.claimSketchSlot(context.Background(), "uuid", 0)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClaimSketchSlot_UpdateError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE _sketch_registry").WillReturnError(fmt.Errorf("constraint error"))
	mock.ExpectRollback()

	_, err := cr.claimSketchSlot(context.Background(), "uuid", 0)
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- loadSlotHighWaterMarks ---

func TestClaimAvailableDimSlotTx_BeginError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)

	mock.ExpectBegin().WillReturnError(fmt.Errorf("cannot begin"))

	_, _, _, err := cr.claimAvailableDimSlotTx(context.Background(), "host", "str", 256)
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestClaimAvailableAggSlotTx_ErrorCases covers transactional error paths in
// claimAvailableAggSlotTx: begin failure and zero-rows-affected CAS race.
func TestClaimAvailableAggSlotTx_ErrorCases(t *testing.T) {
	t.Run("begin_error", func(t *testing.T) {
		db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		defer db.Close() //nolint:errcheck

		cr := testCRWithDB(db)
		mock.ExpectBegin().WillReturnError(fmt.Errorf("cannot begin"))

		_, _, err := cr.claimAvailableAggSlotTx(context.Background(), "level", "error", "EQ", "INT")
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("zero_rows_affected", func(t *testing.T) {
		db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		defer db.Close() //nolint:errcheck

		cr := testCRWithDB(db)
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT column_name, value_type FROM _agg_registry").
			WillReturnRows(sqlmock.NewRows([]string{"column_name", "value_type"}).
				AddRow("agg_f03", "INT"))
		mock.ExpectExec("UPDATE _agg_registry").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()

		col, _, err := cr.claimAvailableAggSlotTx(context.Background(), "level", "error", "EQ", "INT")
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if col != "" {
			t.Errorf("expected empty on CAS race, got %q", col)
		}
	})
}

// TestClaimAvailableAggSlot_EdgeCases covers the none-available path and the
// alter-fails-with-revert path for claimAvailableAggSlot.
func TestClaimAvailableAggSlot_EdgeCases(t *testing.T) {
	t.Run("none_available", func(t *testing.T) {
		db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		defer db.Close() //nolint:errcheck

		cr := testCRWithDB(db)
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT column_name, value_type FROM _agg_registry").
			WillReturnRows(sqlmock.NewRows([]string{"column_name", "value_type"}))
		mock.ExpectRollback()

		col, err := cr.claimAvailableAggSlot(context.Background(), "level", "error", "EQ", "INT")
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if col != "" {
			t.Errorf("expected empty, got %q", col)
		}
	})

	t.Run("alter_fails_revert", func(t *testing.T) {
		db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		defer db.Close() //nolint:errcheck

		cr := testCRWithDB(db)
		cr.isMariaDB = true
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT column_name, value_type FROM _agg_registry").
			WillReturnRows(sqlmock.NewRows([]string{"column_name", "value_type"}).
				AddRow("agg_f05", "INT"))
		mock.ExpectExec("UPDATE _agg_registry").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		mock.ExpectExec("ALTER TABLE").WillReturnError(fmt.Errorf("alter failed"))
		mock.ExpectExec("UPDATE _agg_registry").WillReturnResult(sqlmock.NewResult(0, 1))

		_, err := cr.claimAvailableAggSlot(context.Background(), "avg", "", "AVG", "FLOAT")
		if err == nil {
			t.Fatal("expected error when ALTER fails")
		}
	})
}

// TestAllocateNewAggSlot_SlotsExhausted verifies that allocateNewAggSlot returns
// ErrSlotExhausted when nextAggSlot has reached the cap and no recycled slots exist.
func TestAllocateNewAggSlot_SlotsExhausted(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.nextAggSlot = 100

	mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(
		sqlmock.NewRows([]string{"result"}).AddRow(1))
	mock.ExpectQuery("SELECT column_name FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, value_type FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "value_type"}))
	mock.ExpectRollback()
	mock.ExpectExec("SELECT RELEASE_LOCK").WillReturnResult(sqlmock.NewResult(0, 0))

	_, err := cr.allocateNewAggSlot(context.Background(), "new", "", "SUM", "INT")
	if err != ErrSlotExhausted {
		t.Fatalf("expected ErrSlotExhausted, got %v", err)
	}
}


func TestResolveOrAllocateDims_BatchWithWidening(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	cr := testCRWithDB(db)
	cr.isMariaDB = true
	cr.dimByKey["host"] = &DimRegistryEntry{ColumnName: "dim_f01", DimKey: "host", BaseType: "str", Width: 100}
	cr.dimByColumn["dim_f01"] = cr.dimByKey["host"]

	mock.ExpectExec("ALTER TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE _dim_registry").WillReturnResult(sqlmock.NewResult(0, 1))

	reqs := []DimRequest{{DimKey: "host", BaseType: "str", Width: 500}}
	result, err := cr.ResolveOrAllocateDims(context.Background(), reqs)
	if err != nil {
		t.Fatal(err)
	}
	if result["host"] != "dim_f01" {
		t.Errorf("host = %q, want dim_f01", result["host"])
	}
}

func TestLoadSlotHighWaterMarks_ScanError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	// Return a row that can't be scanned as string
	mock.ExpectQuery("SELECT column_name FROM _dim_registry").WillReturnRows(
		sqlmock.NewRows([]string{"column_name"}).AddRow(nil).RowError(0, fmt.Errorf("scan error")))

	cr := &ColumnRegistry{
		db:          db,
		tableName:   "test_table",
		dimByKey:    make(map[string]*DimRegistryEntry),
		dimByColumn: make(map[string]*DimRegistryEntry),
		aggByKey:    make(map[string]*AggRegistryEntry),
		aggByColumn: make(map[string]*AggRegistryEntry),
		sketchByKey: make(map[string]*SketchRegistryEntry),
		nextDimSlot: 1,
		nextAggSlot: 1,
		log:         zap.NewNop(),
	}

	err := cr.loadSlotHighWaterMarks(context.Background())
	if err == nil {
		t.Fatal("expected error from scan")
	}
}

