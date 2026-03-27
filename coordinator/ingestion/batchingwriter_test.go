package ingestion

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/schema"
)

// mockFlusher is a test mock for BatchFlusher.
type mockFlusher struct {
	flushFunc func(ctx context.Context, tableName string, batch []*metastore.FileRecord) error
	calls     []flushCall
	mu        sync.Mutex
}

type flushCall struct {
	tableName string
	count     int
}

func (m *mockFlusher) FlushBatch(ctx context.Context, tableName string, batch []*metastore.FileRecord) error {
	m.mu.Lock()
	m.calls = append(m.calls, flushCall{tableName: tableName, count: len(batch)})
	m.mu.Unlock()
	if m.flushFunc != nil {
		return m.flushFunc(ctx, tableName, batch)
	}
	return nil
}

func (m *mockFlusher) getCalls() []flushCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]flushCall, len(m.calls))
	copy(result, m.calls)
	return result
}

func (m *mockFlusher) totalRecords() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, c := range m.calls {
		total += c.count
	}
	return total
}

func makeRecord() *metastore.FileRecord {
	return &metastore.FileRecord{
		State:        metastore.StateIRClosed,
		MinTimestamp: time.Now().UnixNano(),
	}
}

func makeRecordWithFlush() (*metastore.FileRecord, chan error) {
	flushed := make(chan error, 1)
	rec := makeRecord()
	rec.Flushed = flushed
	return rec, flushed
}

func TestBatchingWriter_Submit_FlushAtBatchSize(t *testing.T) {
	mock := &mockFlusher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(3),
		WithFlushInterval(10*time.Second), // long interval — batch size should trigger
	)
	bw.testFlusher = mock

	// Submit exactly batch size records
	for i := 0; i < 3; i++ {
		if err := bw.Submit(ctx, "test_table", makeRecord()); err != nil {
			t.Fatalf("Submit error: %v", err)
		}
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	calls := mock.getCalls()
	if len(calls) == 0 {
		t.Fatal("expected at least one flush call")
	}
	if calls[0].count != 3 {
		t.Errorf("flush batch size = %d, want 3", calls[0].count)
	}
}

func TestBatchingWriter_Submit_FlushAtTimeout(t *testing.T) {
	mock := &mockFlusher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(1000), // large batch — timeout should trigger
		WithFlushInterval(50*time.Millisecond),
	)
	bw.testFlusher = mock

	// Submit one record (below batch size)
	if err := bw.Submit(ctx, "test_table", makeRecord()); err != nil {
		t.Fatal(err)
	}

	// Wait for timeout flush
	time.Sleep(200 * time.Millisecond)

	if mock.totalRecords() != 1 {
		t.Errorf("flushed records = %d, want 1", mock.totalRecords())
	}
}

func TestBatchingWriter_NotifyOnSuccess(t *testing.T) {
	mock := &mockFlusher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(1),
		WithFlushInterval(10*time.Second),
	)
	bw.testFlusher = mock

	rec, flushed := makeRecordWithFlush()
	if err := bw.Submit(ctx, "test_table", rec); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-flushed:
		if err != nil {
			t.Errorf("expected nil flush error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for flush notification")
	}
}

func TestBatchingWriter_NotifyOnError(t *testing.T) {
	flushErr := fmt.Errorf("db connection lost")
	mock := &mockFlusher{
		flushFunc: func(_ context.Context, _ string, _ []*metastore.FileRecord) error {
			return flushErr
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(1),
		WithFlushInterval(10*time.Second),
	)
	bw.testFlusher = mock

	rec, flushed := makeRecordWithFlush()
	if err := bw.Submit(ctx, "test_table", rec); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-flushed:
		if err == nil {
			t.Error("expected flush error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for flush notification")
	}
}

func TestBatchingWriter_Submit_ChannelFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a flusher that blocks until signalled, so the channel stays full
	// while we test the non-blocking Submit path.
	flushStarted := make(chan struct{}, 1)
	flushRelease := make(chan struct{})
	mock := &mockFlusher{
		flushFunc: func(ctx context.Context, _ string, _ []*metastore.FileRecord) error {
			select {
			case flushStarted <- struct{}{}:
			default:
			}
			select {
			case <-flushRelease:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(2),
		WithFlushInterval(10*time.Millisecond),
	)
	bw.testFlusher = mock

	// Fill the channel (capacity = batchSize = 2)
	_ = bw.Submit(ctx, "t", makeRecord())
	_ = bw.Submit(ctx, "t", makeRecord())

	// Wait for the tableWriter to drain the channel into its batch and
	// start the blocking flush — no timing assumptions.
	<-flushStarted

	// Channel is now empty (records moved to batch), fill it again
	_ = bw.Submit(ctx, "t", makeRecord())
	_ = bw.Submit(ctx, "t", makeRecord())

	// Next submit should get ErrChannelFull
	err := bw.Submit(ctx, "t", makeRecord())
	if err != ErrChannelFull {
		t.Errorf("expected ErrChannelFull, got %v", err)
	}

	close(flushRelease)
}

func TestBatchingWriter_Stop_DrainsAndFlushes(t *testing.T) {
	flushCount := atomic.Int64{}
	mock := &mockFlusher{
		flushFunc: func(_ context.Context, _ string, batch []*metastore.FileRecord) error {
			flushCount.Add(int64(len(batch)))
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(100),                // large batch — won't trigger by size
		WithFlushInterval(10*time.Second), // long interval — won't trigger by time
	)
	bw.testFlusher = mock

	// Submit a few records
	for i := 0; i < 5; i++ {
		_ = bw.Submit(ctx, "test_table", makeRecord())
	}

	// Stop should drain and flush remaining records
	bw.Stop()

	if flushCount.Load() < 5 {
		t.Errorf("flushed %d records on shutdown, want >= 5", flushCount.Load())
	}
}

func TestBatchingWriter_SubmitWait_Blocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock := &mockFlusher{}
	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(1),
		WithFlushInterval(50*time.Millisecond),
	)
	bw.testFlusher = mock

	// Fill the channel
	_ = bw.Submit(ctx, "t", makeRecord())

	// SubmitWait should block briefly then succeed after the flush timer fires
	done := make(chan error, 1)
	go func() {
		done <- bw.SubmitWait(ctx, "t", makeRecord())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("SubmitWait error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitWait blocked forever")
	}
}

func TestBatchingWriter_MultipleTablesRouteCorrectly(t *testing.T) {
	mock := &mockFlusher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(2),
		WithFlushInterval(50*time.Millisecond),
	)
	bw.testFlusher = mock

	// Submit to two different tables
	_ = bw.Submit(ctx, "table_a", makeRecord())
	_ = bw.Submit(ctx, "table_a", makeRecord())
	_ = bw.Submit(ctx, "table_b", makeRecord())

	time.Sleep(200 * time.Millisecond)

	calls := mock.getCalls()
	tableACounts := 0
	tableBCounts := 0
	for _, c := range calls {
		if c.tableName == "table_a" {
			tableACounts += c.count
		} else if c.tableName == "table_b" {
			tableBCounts += c.count
		}
	}

	if tableACounts != 2 {
		t.Errorf("table_a records = %d, want 2", tableACounts)
	}
	if tableBCounts != 1 {
		t.Errorf("table_b records = %d, want 1", tableBCounts)
	}
}

// --- WithMeter ---

func TestWithMeter_NoopMeter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// WithMeter must not panic and must replace the no-op metrics with
	// instruments from the supplied meter.
	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithMeter(noop.NewMeterProvider().Meter("test")),
	)
	bw.testFlusher = &mockFlusher{}

	// Submit a record so the metric Add paths are exercised.
	if err := bw.Submit(ctx, "t", makeRecord()); err != nil {
		t.Fatalf("Submit error: %v", err)
	}
}

// --- SetRegistry / ensureRegistry ---

func TestSetRegistry_ReplacesPreviousEntry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop())

	// Create two dummy registries via sqlmock so we have two valid pointers.
	reg1 := buildEmptyRegistry(t)
	reg2 := buildEmptyRegistry(t)

	bw.SetRegistry("tbl", reg1)
	bw.SetRegistry("tbl", reg2)

	got, err := bw.ensureRegistry(ctx, "tbl")
	if err != nil {
		t.Fatalf("ensureRegistry error: %v", err)
	}
	if got != reg2 {
		t.Error("expected reg2 to be returned after second SetRegistry call")
	}
}

func TestEnsureRegistry_ReturnsCachedRegistry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop())
	reg := buildEmptyRegistry(t)
	bw.SetRegistry("cached_tbl", reg)

	// First call: returns cached.
	got1, err := bw.ensureRegistry(ctx, "cached_tbl")
	if err != nil {
		t.Fatalf("ensureRegistry error: %v", err)
	}
	if got1 != reg {
		t.Error("expected cached registry to be returned")
	}

	// Second call: double-check fast path (RLock → return early).
	got2, err := bw.ensureRegistry(ctx, "cached_tbl")
	if err != nil {
		t.Fatalf("ensureRegistry second call error: %v", err)
	}
	if got2 != reg {
		t.Error("expected same cached registry on second call")
	}
}

// buildEmptyRegistry creates a *schema.ColumnRegistry via NewColumnRegistry with a
// sqlmock database that returns empty result sets for all initialisation queries.
func buildEmptyRegistry(t *testing.T) *schema.ColumnRegistry {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() }) //nolint:errcheck

	expectEmptyRegistryInit(mock)

	reg, err := schema.NewColumnRegistry(context.Background(), db, "test_tbl", false, zap.NewNop())
	if err != nil {
		t.Fatalf("NewColumnRegistry: %v", err)
	}
	return reg
}

// buildRegistryWithDim creates a *schema.ColumnRegistry pre-loaded with one
// active dim entry (dim_f01 → "host") so ResolveOrAllocateDims uses the fast
// path and never touches the database.
func buildRegistryWithDim(t *testing.T) *schema.ColumnRegistry {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() }) //nolint:errcheck

	// loadSlotHighWaterMarks: SELECT column_name FROM _dim_registry
	mock.ExpectQuery("SELECT column_name FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("dim_f01"))
	// loadSlotHighWaterMarks: SELECT column_name FROM _agg_registry
	mock.ExpectQuery("SELECT column_name FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))

	// load active dims (column_name, base_type, width, dim_key, alias_column)
	mock.ExpectQuery("SELECT column_name, base_type, width, dim_key, alias_column FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{
			"column_name", "base_type", "width", "dim_key", "alias_column",
		}).AddRow("dim_f01", "str", 256, "host", nil))

	// load active aggs
	mock.ExpectQuery("SELECT column_name, agg_key, agg_value, aggregation_type, value_type, alias_column FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{
			"column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column",
		}))

	// load active sketches
	mock.ExpectQuery("SELECT sketch_name, sketch_key FROM _sketch_registry").
		WillReturnRows(sqlmock.NewRows([]string{"sketch_name", "sketch_key"}))

	reg, err := schema.NewColumnRegistry(context.Background(), db, "test_tbl", false, zap.NewNop())
	if err != nil {
		t.Fatalf("NewColumnRegistry: %v", err)
	}
	return reg
}

// expectEmptyRegistryInit sets up the five mock queries that NewColumnRegistry
// issues during initialisation, all returning empty result sets.
func expectEmptyRegistryInit(mock sqlmock.Sqlmock) {
	// loadSlotHighWaterMarks: dim
	mock.ExpectQuery("SELECT column_name FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	// loadSlotHighWaterMarks: agg
	mock.ExpectQuery("SELECT column_name FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	// load active dims
	mock.ExpectQuery("SELECT column_name, base_type, width, dim_key, alias_column FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{
			"column_name", "base_type", "width", "dim_key", "alias_column",
		}))
	// load active aggs
	mock.ExpectQuery("SELECT column_name, agg_key, agg_value, aggregation_type, value_type, alias_column FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{
			"column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column",
		}))
	// load active sketches
	mock.ExpectQuery("SELECT sketch_name, sketch_key FROM _sketch_registry").
		WillReturnRows(sqlmock.NewRows([]string{"sketch_name", "sketch_key"}))
}

// --- resolveAndRemapBatch ---

func TestResolveAndRemapBatch_EmptyBatch(t *testing.T) {
	reg := buildEmptyRegistry(t)
	err := resolveAndRemapBatch(context.Background(), reg, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestResolveAndRemapBatch_NoDimsOrAggs(t *testing.T) {
	reg := buildEmptyRegistry(t)
	batch := []*metastore.FileRecord{
		{State: metastore.StateIRClosed, MinTimestamp: time.Now().UnixNano()},
		{State: metastore.StateIRClosed, MinTimestamp: time.Now().UnixNano()},
	}
	if err := resolveAndRemapBatch(context.Background(), reg, batch); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestResolveAndRemapBatch_RemapsDimsFromCache(t *testing.T) {
	// Registry pre-loaded with dim_f01 → "host"; ResolveOrAllocateDims uses
	// the fast path (in-memory map) and never touches the DB.
	reg := buildRegistryWithDim(t)

	rec := &metastore.FileRecord{
		State:        metastore.StateIRClosed,
		MinTimestamp: time.Now().UnixNano(),
		DimMeta: []metastore.DimMeta{
			{Key: "host", BaseType: "str", Width: 256},
		},
		Dims: map[string]any{"host": "web-01"},
	}
	batch := []*metastore.FileRecord{rec}

	if err := resolveAndRemapBatch(context.Background(), reg, batch); err != nil {
		t.Fatalf("resolveAndRemapBatch error: %v", err)
	}

	// "host" should be remapped to physical column "dim_f01".
	if _, ok := rec.Dims["dim_f01"]; !ok {
		t.Errorf("expected Dims[\"dim_f01\"], got %v", rec.Dims)
	}
	if _, ok := rec.Dims["host"]; ok {
		t.Error("logical key \"host\" should have been removed after remap")
	}
}

func TestResolveAndRemapBatch_AggsRemappedFromCache(t *testing.T) {
	// Build registry with one active agg entry so the fast path is used.
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close() //nolint:errcheck

	// loadSlotHighWaterMarks
	mock.ExpectQuery("SELECT column_name FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	mock.ExpectQuery("SELECT column_name FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("agg_f01"))

	// load active dims (empty)
	mock.ExpectQuery("SELECT column_name, base_type, width, dim_key, alias_column FROM _dim_registry").
		WillReturnRows(sqlmock.NewRows([]string{
			"column_name", "base_type", "width", "dim_key", "alias_column",
		}))

	// load active aggs — one entry: EQ\x00level\x00error → agg_f01
	aggKey := schema.AggCacheKey("level", "error", "EQ")
	mock.ExpectQuery("SELECT column_name, agg_key, agg_value, aggregation_type, value_type, alias_column FROM _agg_registry").
		WillReturnRows(sqlmock.NewRows([]string{
			"column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column",
		}).AddRow("agg_f01", "level", "error", "EQ", "INT", nil))

	// load active sketches (empty)
	mock.ExpectQuery("SELECT sketch_name, sketch_key FROM _sketch_registry").
		WillReturnRows(sqlmock.NewRows([]string{"sketch_name", "sketch_key"}))

	reg, err := schema.NewColumnRegistry(context.Background(), db, "test_tbl", false, zap.NewNop())
	if err != nil {
		t.Fatalf("NewColumnRegistry: %v", err)
	}

	rec := &metastore.FileRecord{
		State:        metastore.StateIRClosed,
		MinTimestamp: time.Now().UnixNano(),
		AggMeta: []metastore.AggMeta{
			{Key: "level", Value: "error", Type: "EQ", ValueType: "INT"},
		},
		Aggs: map[string]any{aggKey: int64(5)},
	}

	if err := resolveAndRemapBatch(context.Background(), reg, []*metastore.FileRecord{rec}); err != nil {
		t.Fatalf("resolveAndRemapBatch error: %v", err)
	}

	if _, ok := rec.Aggs["agg_f01"]; !ok {
		t.Errorf("expected Aggs[\"agg_f01\"], got %v", rec.Aggs)
	}
}

// --- encodeSketchExt ---

func TestEncodeSketchExt_Success(t *testing.T) {
	// Encode a valid msgpack snapshot (type + data fields).
	type sketchSnapshot struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	}
	snapBytes, err := msgpack.Marshal(&sketchSnapshot{
		Type: "parquet_sbbf_xxhash64",
		Data: []byte{0xDE, 0xAD},
	})
	if err != nil {
		t.Fatalf("msgpack.Marshal: %v", err)
	}

	rec := &metastore.FileRecord{
		State:        metastore.StateIRClosed,
		MinTimestamp: time.Now().UnixNano(),
		Sketches:     map[string][]byte{"uuid": snapBytes},
	}
	sketchMap := map[string]string{"uuid": "s01"}

	if err := encodeSketchExt(rec, sketchMap); err != nil {
		t.Fatalf("encodeSketchExt error: %v", err)
	}
	if rec.SketchSetValue != "s01" {
		t.Errorf("SketchSetValue = %q, want \"s01\"", rec.SketchSetValue)
	}
	if len(rec.ExtData) == 0 {
		t.Error("ExtData should be non-empty after encoding")
	}
}

func TestEncodeSketchExt_MultipleSketchesSorted(t *testing.T) {
	type sketchSnapshot struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	}
	makeSnap := func(typ string) []byte {
		b, _ := msgpack.Marshal(&sketchSnapshot{Type: typ, Data: []byte{0x01}})
		return b
	}

	rec := &metastore.FileRecord{
		State:        metastore.StateIRClosed,
		MinTimestamp: time.Now().UnixNano(),
		Sketches: map[string][]byte{
			"b_field": makeSnap("parquet_sbbf_xxhash64"),
			"a_field": makeSnap("parquet_sbbf_xxhash64"),
		},
	}
	sketchMap := map[string]string{
		"a_field": "s02",
		"b_field": "s01",
	}

	if err := encodeSketchExt(rec, sketchMap); err != nil {
		t.Fatalf("encodeSketchExt error: %v", err)
	}
	// Members should be sorted: s01,s02
	if rec.SketchSetValue != "s01,s02" {
		t.Errorf("SketchSetValue = %q, want \"s01,s02\"", rec.SketchSetValue)
	}
}

func TestEncodeSketchExt_UnmarshalError(t *testing.T) {
	rec := &metastore.FileRecord{
		State:        metastore.StateIRClosed,
		MinTimestamp: time.Now().UnixNano(),
		Sketches:     map[string][]byte{"uuid": []byte{0xFF, 0xFF, 0xFF}},
	}
	sketchMap := map[string]string{"uuid": "s01"}

	err := encodeSketchExt(rec, sketchMap)
	if err == nil {
		t.Fatal("expected error for invalid msgpack data")
	}
}

func TestEncodeSketchExt_SkipsUnresolvedKey(t *testing.T) {
	type sketchSnapshot struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	}
	snapBytes, _ := msgpack.Marshal(&sketchSnapshot{Type: "t", Data: []byte{0x01}})

	rec := &metastore.FileRecord{
		State:        metastore.StateIRClosed,
		MinTimestamp: time.Now().UnixNano(),
		Sketches: map[string][]byte{
			"known":   snapBytes,
			"unknown": snapBytes,
		},
	}
	// "unknown" key is absent from sketchMap.
	sketchMap := map[string]string{"known": "s01"}

	if err := encodeSketchExt(rec, sketchMap); err != nil {
		t.Fatalf("encodeSketchExt error: %v", err)
	}
	if rec.SketchSetValue != "s01" {
		t.Errorf("SketchSetValue = %q, want \"s01\"", rec.SketchSetValue)
	}
}

// --- FlushBatch (dbFlusher) ---

func TestDbFlusher_FlushBatch_EmptyBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close() //nolint:errcheck

	bw := NewBatchingWriter(ctx, db, false, zap.NewNop())
	reg := buildEmptyRegistry(t)
	bw.SetRegistry("flush_tbl", reg)

	// No DB queries expected: NewFileRecords validates the table name but
	// UpsertBatch returns early for empty batches.
	_ = mock

	f := &dbFlusher{bw: bw, tableName: "flush_tbl"}
	if err := f.FlushBatch(ctx, "flush_tbl", nil); err != nil {
		t.Errorf("FlushBatch(empty) error: %v", err)
	}
}

func TestDbFlusher_FlushBatch_InvalidTableName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, _, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close() //nolint:errcheck

	bw := NewBatchingWriter(ctx, db, false, zap.NewNop())

	// An invalid table name (contains spaces) must make NewFileRecords fail.
	f := &dbFlusher{bw: bw, tableName: "bad table name"}
	err = f.FlushBatch(ctx, "bad table name", []*metastore.FileRecord{makeRecord()})
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestDbFlusher_FlushBatch_RegistryError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close() //nolint:errcheck

	bw := NewBatchingWriter(ctx, db, false, zap.NewNop())
	// Do NOT call SetRegistry — ensureRegistry will try to create one.
	// Make the DB query fail so NewColumnRegistry returns an error.
	mock.ExpectQuery("SELECT column_name FROM _dim_registry").
		WillReturnError(fmt.Errorf("db down"))

	f := &dbFlusher{bw: bw, tableName: "noregistry_tbl"}
	err = f.FlushBatch(ctx, "noregistry_tbl", []*metastore.FileRecord{makeRecord()})
	if err == nil {
		t.Fatal("expected error when registry creation fails")
	}
}

// --- SubmitWait context cancellation ---

func TestBatchingWriter_SubmitWait_ContextCancelled(t *testing.T) {
	// Use a flusher that blocks so the channel fills and SubmitWait must
	// block, then cancel the context to trigger the ctx.Done() branch.
	flushStarted := make(chan struct{}, 1)
	flushRelease := make(chan struct{})
	mock := &mockFlusher{
		flushFunc: func(ctx context.Context, _ string, _ []*metastore.FileRecord) error {
			select {
			case flushStarted <- struct{}{}:
			default:
			}
			select {
			case <-flushRelease:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}

	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	bw := NewBatchingWriter(parentCtx, nil, false, zap.NewNop(),
		WithBatchSize(1),
		WithFlushInterval(10*time.Second),
	)
	bw.testFlusher = mock

	// Fill the channel (capacity = batchSize = 1).
	_ = bw.Submit(parentCtx, "t", makeRecord())

	// Wait for the flush to start so the channel is empty but the writer is blocked.
	<-flushStarted

	// Fill the channel again so SubmitWait will block.
	_ = bw.Submit(parentCtx, "t", makeRecord())

	// Cancel a short-lived context and pass it to SubmitWait.
	shortCtx, shortCancel := context.WithCancel(parentCtx)

	done := make(chan error, 1)
	go func() {
		done <- bw.SubmitWait(shortCtx, "t", makeRecord())
	}()

	// Give the goroutine time to block, then cancel.
	time.Sleep(20 * time.Millisecond)
	shortCancel()

	select {
	case err := <-done:
		if err == nil {
			t.Error("expected context cancellation error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitWait did not return after context cancellation")
	}

	close(flushRelease)
}
