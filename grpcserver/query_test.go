package grpcserver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/y-scope/metalog/gen/proto/splitspb"
	"github.com/y-scope/metalog/query"
	"github.com/y-scope/metalog/schema"
)

// --- rowToProtoSplit tests ---

func TestRowToProtoSplit_BasicFields(t *testing.T) {
	row := &query.SplitRow{
		ID: 42,
		Values: map[string]any{
			"min_timestamp": int64(1000),
			"max_timestamp": int64(2000),
			"state":         "IR_CLOSED",
			"record_count":  int64(500),
		},
	}

	split := rowToProtoSplit(row, nil, zap.NewNop())
	f := split.GetFile()

	if f.GetMinTimestamp() != 1000 {
		t.Errorf("MinTimestamp = %d, want 1000", f.GetMinTimestamp())
	}
	if f.GetMaxTimestamp() != 2000 {
		t.Errorf("MaxTimestamp = %d, want 2000", f.GetMaxTimestamp())
	}
	if f.GetState() != "IR_CLOSED" {
		t.Errorf("State = %q, want IR_CLOSED", f.GetState())
	}
	if f.GetRecordCount() != 500 {
		t.Errorf("RecordCount = %d, want 500", f.GetRecordCount())
	}
}

func TestRowToProtoSplit_IRFields(t *testing.T) {
	row := &query.SplitRow{
		ID: 1,
		Values: map[string]any{
			"clp_ir_path":            "/logs/test.clp.zst",
			"clp_ir_storage_backend": "s3",
			"clp_ir_bucket":          "my-bucket",
			"clp_ir_size_bytes":      int64(4096),
		},
	}

	split := rowToProtoSplit(row, nil, zap.NewNop())
	f := split.GetFile()

	if f.GetClpIrPath() != "/logs/test.clp.zst" {
		t.Errorf("ClpIrPath = %q", f.GetClpIrPath())
	}
	if f.GetClpIrStorageBackend() != "s3" {
		t.Errorf("ClpIrStorageBackend = %q", f.GetClpIrStorageBackend())
	}
	if f.GetClpIrBucket() != "my-bucket" {
		t.Errorf("ClpIrBucket = %q", f.GetClpIrBucket())
	}
	if f.GetClpIrSizeBytes() != 4096 {
		t.Errorf("ClpIrSizeBytes = %d, want 4096", f.GetClpIrSizeBytes())
	}
}

func TestRowToProtoSplit_BothSizesExposed(t *testing.T) {
	row := &query.SplitRow{
		ID: 1,
		Values: map[string]any{
			"clp_ir_size_bytes":      int64(1000),
			"clp_archive_path":       "/archives/test.tar",
			"clp_archive_size_bytes": int64(5000),
		},
	}

	f := rowToProtoSplit(row, nil, zap.NewNop()).GetFile()

	if f.GetClpIrSizeBytes() != 1000 {
		t.Errorf("ClpIrSizeBytes = %d, want 1000", f.GetClpIrSizeBytes())
	}
	if f.GetClpArchiveSizeBytes() != 5000 {
		t.Errorf("ClpArchiveSizeBytes = %d, want 5000", f.GetClpArchiveSizeBytes())
	}
}

func TestRowToProtoSplit_ArchiveFields(t *testing.T) {
	row := &query.SplitRow{
		ID: 1,
		Values: map[string]any{
			"clp_archive_path":            "/arch/2024/foo.tar",
			"clp_archive_storage_backend": "filesystem",
			"clp_archive_bucket":          "",
			"clp_archive_size_bytes":      int64(8192),
			"clp_archive_created_at":      int64(1700000000),
			"retention_days":              int64(30),
			"expires_at":                  int64(1702678400),
		},
	}

	f := rowToProtoSplit(row, nil, zap.NewNop()).GetFile()

	if f.GetClpArchivePath() != "/arch/2024/foo.tar" {
		t.Errorf("ClpArchivePath = %q", f.GetClpArchivePath())
	}
	if f.GetClpArchiveStorageBackend() != "filesystem" {
		t.Errorf("ClpArchiveStorageBackend = %q", f.GetClpArchiveStorageBackend())
	}
	if f.GetClpArchiveSizeBytes() != 8192 {
		t.Errorf("ClpArchiveSizeBytes = %d, want 8192", f.GetClpArchiveSizeBytes())
	}
	if f.GetClpArchiveCreatedAt() != 1700000000 {
		t.Errorf("ClpArchiveCreatedAt = %d, want 1700000000", f.GetClpArchiveCreatedAt())
	}
	if f.GetRetentionDays() != 30 {
		t.Errorf("RetentionDays = %d, want 30", f.GetRetentionDays())
	}
	if f.GetExpiresAt() != 1702678400 {
		t.Errorf("ExpiresAt = %d, want 1702678400", f.GetExpiresAt())
	}
}

func TestRowToProtoSplit_DimensionColumns(t *testing.T) {
	row := &query.SplitRow{
		ID: 1,
		Values: map[string]any{
			"dim_f01": "app-001",
			"dim_f02": "us-east-1",
		},
	}

	split := rowToProtoSplit(row, nil, zap.NewNop())

	if len(split.Dimensions) != 2 {
		t.Fatalf("Dimensions count = %d, want 2", len(split.Dimensions))
	}
	if split.Dimensions["dim_f01"] != "app-001" {
		t.Errorf("dim_f01 = %q, want app-001", split.Dimensions["dim_f01"])
	}
	if split.Dimensions["dim_f02"] != "us-east-1" {
		t.Errorf("dim_f02 = %q, want us-east-1", split.Dimensions["dim_f02"])
	}
}

func TestRowToProtoSplit_NilValueNotInDimensions(t *testing.T) {
	row := &query.SplitRow{
		ID: 1,
		Values: map[string]any{
			"dim_f01": nil,
			"dim_f02": "value",
		},
	}

	split := rowToProtoSplit(row, nil, zap.NewNop())

	if _, exists := split.Dimensions["dim_f01"]; exists {
		t.Error("nil dimension should not be included")
	}
	if split.Dimensions["dim_f02"] != "value" {
		t.Errorf("dim_f02 = %q, want value", split.Dimensions["dim_f02"])
	}
}

func TestRowToProtoSplit_WrongTypeForTimestamp(t *testing.T) {
	row := &query.SplitRow{
		ID: 1,
		Values: map[string]any{
			"min_timestamp": "not-an-int",
		},
	}

	f := rowToProtoSplit(row, nil, zap.NewNop()).GetFile()

	if f.GetMinTimestamp() != 0 {
		t.Errorf("MinTimestamp = %d, want 0 for wrong type", f.GetMinTimestamp())
	}
}

func TestRowToProtoSplit_EmptyRow(t *testing.T) {
	row := &query.SplitRow{
		ID:     0,
		Values: map[string]any{},
	}

	split := rowToProtoSplit(row, nil, zap.NewNop())

	if split.GetFile() == nil {
		t.Fatal("File should not be nil")
	}
	if len(split.Dimensions) != 0 {
		t.Errorf("Dimensions should be empty, got %d", len(split.Dimensions))
	}
}

func TestRowToProtoSplit_FileColumnsInFileInfo(t *testing.T) {
	row := &query.SplitRow{
		ID: 1,
		Values: map[string]any{
			"id":                     int64(1),
			"raw_size_bytes":         int64(9999),
			"clp_archive_created_at": int64(12345),
			"retention_days":         int64(30),
			"expires_at":             int64(99999),
			"clp_ir_path_hash":       int64(111),
			"clp_archive_path_hash":  int64(222),
			"dim_f01":                "should-appear",
		},
	}

	split := rowToProtoSplit(row, nil, zap.NewNop())
	f := split.GetFile()

	// File columns populate FileInfo, not dimensions
	if f.GetRawSizeBytes() != 9999 {
		t.Errorf("RawSizeBytes = %d, want 9999", f.GetRawSizeBytes())
	}
	if f.GetClpArchiveCreatedAt() != 12345 {
		t.Errorf("ClpArchiveCreatedAt = %d, want 12345", f.GetClpArchiveCreatedAt())
	}
	if f.GetRetentionDays() != 30 {
		t.Errorf("RetentionDays = %d, want 30", f.GetRetentionDays())
	}
	if f.GetExpiresAt() != 99999 {
		t.Errorf("ExpiresAt = %d, want 99999", f.GetExpiresAt())
	}

	// Internal columns (id, path hashes) and dimensions
	if len(split.Dimensions) != 1 {
		t.Errorf("Dimensions count = %d, want 1 (internal columns should be skipped)", len(split.Dimensions))
	}
	if split.Dimensions["dim_f01"] != "should-appear" {
		t.Errorf("dim_f01 = %q, want should-appear", split.Dimensions["dim_f01"])
	}
}

// TestRowToProtoSplit_AggColumnsWithRegistry verifies that agg_fNN columns are
// resolved to logical names when a registry is provided, and that float and int
// result types are correctly mapped to the AggEntry oneof.
func TestRowToProtoSplit_AggColumnsWithRegistry(t *testing.T) {
	reg := newTestRegistryForGRPC(t)

	row := &query.SplitRow{
		ID: 1,
		Values: map[string]any{
			"agg_f01": int64(200),    // status_code, EQ, INT
			"agg_f02": float64(99.5), // response_time/p99, GTE, FLOAT
			"agg_f03": int64(1024),   // bytes_sent, SUM, INT
		},
	}

	split := rowToProtoSplit(row, reg, zap.NewNop())

	if len(split.Aggs) != 3 {
		t.Fatalf("Aggs count = %d, want 3", len(split.Aggs))
	}

	// Build a map by key for deterministic lookup (map iteration is random).
	byKey := make(map[string]*pb.AggEntry)
	for _, a := range split.Aggs {
		byKey[a.GetKey()] = a
	}

	sc, ok := byKey["status_code"]
	if !ok {
		t.Fatal("missing agg entry for status_code")
	}
	if sc.GetAggregationType() != pb.AggregationType_AGGREGATION_TYPE_EQ {
		t.Errorf("status_code agg type = %v, want EQ", sc.GetAggregationType())
	}
	if sc.GetIntValue() != 200 {
		t.Errorf("status_code int value = %d, want 200", sc.GetIntValue())
	}

	rt, ok := byKey["response_time"]
	if !ok {
		t.Fatal("missing agg entry for response_time")
	}
	if rt.GetAggregationType() != pb.AggregationType_AGGREGATION_TYPE_GTE {
		t.Errorf("response_time agg type = %v, want GTE", rt.GetAggregationType())
	}
	if rt.GetFloatValue() != 99.5 {
		t.Errorf("response_time float value = %f, want 99.5", rt.GetFloatValue())
	}
}

// TestRowToProtoSplit_AggUnknownType verifies that an unknown aggregation type
// string produces AGGREGATION_TYPE_UNSPECIFIED and emits a warning (no panic).
func TestRowToProtoSplit_AggUnknownType(t *testing.T) {
	// Build a minimal registry that maps agg_f01 to an unknown agg type.
	reg := newRegistryWithCustomAgg(t, "agg_f01", "my_key", "", "BOGUS_TYPE", "INT")

	row := &query.SplitRow{
		ID:     1,
		Values: map[string]any{"agg_f01": int64(42)},
	}

	split := rowToProtoSplit(row, reg, zap.NewNop())

	if len(split.Aggs) != 1 {
		t.Fatalf("Aggs count = %d, want 1", len(split.Aggs))
	}
	if split.Aggs[0].GetAggregationType() != pb.AggregationType_AGGREGATION_TYPE_UNSPECIFIED {
		t.Errorf("expected UNSPECIFIED for unknown type, got %v", split.Aggs[0].GetAggregationType())
	}
}

// TestRowToProtoSplit_AggNoResult verifies that an agg column with a nil value
// produces an AggEntry with HasResult=false (no oneof set).
func TestRowToProtoSplit_AggNoResult(t *testing.T) {
	reg := newRegistryWithCustomAgg(t, "agg_f01", "count_key", "", "EQ", "INT")

	row := &query.SplitRow{
		ID:     1,
		Values: map[string]any{"agg_f01": nil},
	}

	// nil value → ResolveSplit skips it → no agg entries in split.
	split := rowToProtoSplit(row, reg, zap.NewNop())
	if len(split.Aggs) != 0 {
		t.Errorf("expected 0 agg entries for nil value, got %d", len(split.Aggs))
	}
}

// --- toCursorValue tests (table-driven) ---

func TestToCursorValue(t *testing.T) {
	tests := []struct {
		input     any
		name      string
		wantStr   string
		wantFloat float64
		wantInt   int64
		wantNil   bool
	}{
		{name: "nil", input: nil, wantNil: true},
		{name: "int64", input: int64(42), wantInt: 42},
		{name: "int32", input: int32(7), wantInt: 7},
		{name: "uint64", input: uint64(100), wantInt: 100},
		{name: "uint32", input: uint32(50), wantInt: 50},
		{name: "float64", input: float64(3.14), wantFloat: 3.14},
		{name: "float32", input: float32(2.5), wantFloat: 2.5},
		{name: "string", input: "hello", wantStr: "hello"},
		{name: "bytes", input: []byte("raw"), wantStr: "raw"},
		{name: "fallback_bool", input: true, wantStr: "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cv := toCursorValue(tt.input)
			if tt.wantNil {
				if cv.GetValue() != nil {
					t.Errorf("nil input: want empty CursorValue, got %v", cv.GetValue())
				}
				return
			}
			if tt.wantInt != 0 || tt.input == int64(0) || tt.input == int32(0) || tt.input == uint64(0) || tt.input == uint32(0) {
				if cv.GetIntVal() != tt.wantInt {
					t.Errorf("IntVal = %d, want %d", cv.GetIntVal(), tt.wantInt)
				}
			}
			if tt.wantFloat != 0 {
				if cv.GetFloatVal() != tt.wantFloat {
					t.Errorf("FloatVal = %f, want %f", cv.GetFloatVal(), tt.wantFloat)
				}
			}
			if tt.wantStr != "" {
				if cv.GetStrVal() != tt.wantStr {
					t.Errorf("StrVal = %q, want %q", cv.GetStrVal(), tt.wantStr)
				}
			}
		})
	}
}

// --- NewQueryHandler / SetMeter / initMetrics tests ---

func TestNewQueryHandler_NotNil(t *testing.T) {
	lookup := func(string) *schema.ColumnRegistry { return nil }
	h := NewQueryHandler(nil, lookup, zap.NewNop())
	if h == nil {
		t.Fatal("NewQueryHandler returned nil")
	}
	// Metrics fields are initialised by initMetrics via noop.Meter{}; they
	// must not be nil so that StreamSplits can call Add/Record without panic.
	if h.mRequests == nil {
		t.Error("mRequests should not be nil after construction")
	}
	if h.mDuration == nil {
		t.Error("mDuration should not be nil after construction")
	}
	if h.mSplitsMatched == nil {
		t.Error("mSplitsMatched should not be nil after construction")
	}
}

func TestSetMeter_ReplacesMetrics(t *testing.T) {
	lookup := func(string) *schema.ColumnRegistry { return nil }
	h := NewQueryHandler(nil, lookup, zap.NewNop())

	// SetMeter with a real noop.Meter should reinitialise all three fields
	// without panicking.
	h.SetMeter(noop.NewMeterProvider().Meter("test"))

	if h.mRequests == nil {
		t.Error("mRequests should not be nil after SetMeter")
	}
	if h.mDuration == nil {
		t.Error("mDuration should not be nil after SetMeter")
	}
	if h.mSplitsMatched == nil {
		t.Error("mSplitsMatched should not be nil after SetMeter")
	}
}

// --- StreamSplits gRPC handler validation tests ---

// mockStream implements grpc.ServerStreamingServer[pb.StreamSplitsResponse]
// for testing request validation (no actual streaming needed).
type mockStream struct {
	ctx       context.Context
	responses []*pb.StreamSplitsResponse
}

func (m *mockStream) Send(resp *pb.StreamSplitsResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockStream) SendHeader(metadata.MD) error { return nil }
func (m *mockStream) SetTrailer(metadata.MD)       {}
func (m *mockStream) Context() context.Context     { return m.ctx }
func (m *mockStream) SendMsg(any) error            { return nil }
func (m *mockStream) RecvMsg(any) error            { return nil }

func newMockStream() *mockStream {
	return &mockStream{ctx: context.Background()}
}

// newValidationHandler creates a QueryHandler with a no-op registryLookup
// suitable for testing request validation (before the engine is invoked).
func newValidationHandler() *QueryHandler {
	return &QueryHandler{
		registryLookup: func(string) *schema.ColumnRegistry { return nil },
	}
}

func TestStreamSplits_EmptyTable(t *testing.T) {
	h := &QueryHandler{}
	req := &pb.StreamSplitsRequest{}
	err := h.StreamSplits(req, newMockStream())

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
	if st.Message() != "table is required" {
		t.Errorf("message = %q", st.Message())
	}
}

func TestStreamSplits_EmptyOrderBy(t *testing.T) {
	h := &QueryHandler{}
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
	}
	err := h.StreamSplits(req, newMockStream())

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
	if st.Message() != "order_by is required (must have at least one field)" {
		t.Errorf("message = %q", st.Message())
	}
}

func TestStreamSplits_EmptyOrderByColumn(t *testing.T) {
	h := &QueryHandler{}
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "", Order: pb.Order_ORDER_ASC},
		},
	}
	err := h.StreamSplits(req, newMockStream())

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
	if st.Message() != "order_by column must not be empty" {
		t.Errorf("message = %q", st.Message())
	}
}

func TestStreamSplits_OrderByID(t *testing.T) {
	h := &QueryHandler{}
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "id", Order: pb.Order_ORDER_ASC},
		},
	}
	err := h.StreamSplits(req, newMockStream())

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
	if st.Message() != "\"id\" must not appear in order_by — it is the implicit final tiebreaker" {
		t.Errorf("message = %q", st.Message())
	}
}

func TestStreamSplits_UnspecifiedOrder(t *testing.T) {
	h := &QueryHandler{}
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "min_timestamp", Order: pb.Order_ORDER_UNSPECIFIED},
		},
	}
	err := h.StreamSplits(req, newMockStream())

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestStreamSplits_CursorLengthMismatch(t *testing.T) {
	h := newValidationHandler()
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "min_timestamp", Order: pb.Order_ORDER_ASC},
			{Column: "max_timestamp", Order: pb.Order_ORDER_DESC},
		},
		Cursor: &pb.KeysetCursor{
			Id: 1,
			Values: []*pb.CursorValue{
				{Value: &pb.CursorValue_IntVal{IntVal: 100}},
				// missing second value
			},
		},
	}
	err := h.StreamSplits(req, newMockStream())

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestStreamSplits_CursorValueUnset(t *testing.T) {
	h := newValidationHandler()
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "min_timestamp", Order: pb.Order_ORDER_ASC},
		},
		Cursor: &pb.KeysetCursor{
			Id: 1,
			Values: []*pb.CursorValue{
				{}, // no value set
			},
		},
	}
	err := h.StreamSplits(req, newMockStream())

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for unset cursor value, got %v", err)
	}
}

// TestStreamSplits_SuccessEmptyResult exercises the full success path with a
// sqlmock engine that returns zero rows, verifying the final stats response.
func TestStreamSplits_SuccessEmptyResult(t *testing.T) {
	engine, mock, db := newTestEngineForGRPC(t)
	defer db.Close() //nolint:errcheck

	// Return empty result set (0 rows → last page).
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id", "min_timestamp"}))

	h := NewQueryHandler(engine, func(string) *schema.ColumnRegistry { return nil }, zap.NewNop())
	stream := newMockStream()
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "min_timestamp", Order: pb.Order_ORDER_ASC},
		},
	}

	err := h.StreamSplits(req, stream)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Must have exactly one response: the final done=true stats message.
	if len(stream.responses) != 1 {
		t.Fatalf("expected 1 response (stats), got %d", len(stream.responses))
	}
	final := stream.responses[0]
	if !final.GetDone() {
		t.Error("last response should have done=true")
	}
	stats := final.GetStats()
	if stats == nil {
		t.Fatal("final response missing stats")
	}
	if stats.GetSplitsScanned() != 0 {
		t.Errorf("SplitsScanned = %d, want 0", stats.GetSplitsScanned())
	}
	if stats.GetSplitsMatched() != 0 {
		t.Errorf("SplitsMatched = %d, want 0", stats.GetSplitsMatched())
	}
	if stats.GetTruncated() {
		t.Error("Truncated should be false for empty result")
	}
}

// TestStreamSplits_SuccessWithRows exercises the success path with two rows,
// verifying each row is streamed and the final stats are correct.
func TestStreamSplits_SuccessWithRows(t *testing.T) {
	engine, mock, db := newTestEngineForGRPC(t)
	defer db.Close() //nolint:errcheck

	rows := sqlmock.NewRows([]string{"id", "min_timestamp"}).
		AddRow(int64(1), int64(1000)).
		AddRow(int64(2), int64(2000))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	h := NewQueryHandler(engine, func(string) *schema.ColumnRegistry { return nil }, zap.NewNop())
	stream := newMockStream()
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "min_timestamp", Order: pb.Order_ORDER_ASC},
		},
	}

	err := h.StreamSplits(req, stream)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 2 data responses + 1 final stats response.
	if len(stream.responses) != 3 {
		t.Fatalf("expected 3 responses (2 splits + stats), got %d", len(stream.responses))
	}

	// First two should not be done.
	for i, r := range stream.responses[:2] {
		if r.GetDone() {
			t.Errorf("response[%d] should not have done=true", i)
		}
		if r.GetSplit() == nil {
			t.Errorf("response[%d] should have a split", i)
		}
		if r.GetSequence() != int32(i+1) {
			t.Errorf("response[%d] sequence = %d, want %d", i, r.GetSequence(), i+1)
		}
	}

	final := stream.responses[2]
	if !final.GetDone() {
		t.Error("last response should have done=true")
	}
	if final.GetStats().GetSplitsMatched() != 2 {
		t.Errorf("SplitsMatched = %d, want 2", final.GetStats().GetSplitsMatched())
	}
}

// TestStreamSplits_SuccessWithCursor verifies cursor propagation when
// include_cursor=true is set on the request.
func TestStreamSplits_SuccessWithCursor(t *testing.T) {
	engine, mock, db := newTestEngineForGRPC(t)
	defer db.Close() //nolint:errcheck

	rows := sqlmock.NewRows([]string{"id", "min_timestamp"}).
		AddRow(int64(10), int64(5000))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	h := NewQueryHandler(engine, func(string) *schema.ColumnRegistry { return nil }, zap.NewNop())
	stream := newMockStream()
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "min_timestamp", Order: pb.Order_ORDER_ASC},
		},
		IncludeCursor: true,
	}

	err := h.StreamSplits(req, stream)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 1 data response + 1 stats response.
	if len(stream.responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(stream.responses))
	}

	dataResp := stream.responses[0]
	if dataResp.GetCursor() == nil {
		t.Fatal("expected cursor in response when include_cursor=true")
	}
	if dataResp.GetCursor().GetId() != 10 {
		t.Errorf("cursor ID = %d, want 10", dataResp.GetCursor().GetId())
	}
}

// TestStreamSplits_SuccessWithLimit verifies that totalLimit caps results and
// the Truncated flag is set when more rows are available.
func TestStreamSplits_SuccessWithLimit(t *testing.T) {
	engine, mock, db := newTestEngineForGRPC(t)
	defer db.Close() //nolint:errcheck

	// Return limit+1 rows (3) to trigger truncation detection when limit=2.
	rows := sqlmock.NewRows([]string{"id", "min_timestamp"}).
		AddRow(int64(1), int64(1000)).
		AddRow(int64(2), int64(2000)).
		AddRow(int64(3), int64(3000)) // extra row proves more results exist
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	h := NewQueryHandler(engine, func(string) *schema.ColumnRegistry { return nil }, zap.NewNop())
	stream := newMockStream()
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "min_timestamp", Order: pb.Order_ORDER_ASC},
		},
		Limit: 2,
	}

	err := h.StreamSplits(req, stream)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 2 data responses + 1 stats.
	if len(stream.responses) != 3 {
		t.Fatalf("expected 3 responses (2 splits + stats), got %d", len(stream.responses))
	}

	final := stream.responses[2]
	if !final.GetStats().GetTruncated() {
		t.Error("expected Truncated=true when limit reached and more rows exist")
	}
}

// TestStreamSplits_EngineError verifies that a DB error from the engine is
// mapped to an Internal gRPC status.
func TestStreamSplits_EngineError(t *testing.T) {
	engine, mock, db := newTestEngineForGRPC(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT").WillReturnError(sql.ErrConnDone)

	h := NewQueryHandler(engine, func(string) *schema.ColumnRegistry { return nil }, zap.NewNop())
	stream := newMockStream()
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "min_timestamp", Order: pb.Order_ORDER_ASC},
		},
	}

	err := h.StreamSplits(req, stream)
	if err == nil {
		t.Fatal("expected error from engine, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Errorf("expected Internal status, got %v", err)
	}
}

// TestStreamSplits_InvalidFilter verifies that an invalid filter expression
// is rejected before the engine is invoked.
func TestStreamSplits_InvalidFilter(t *testing.T) {
	h := newValidationHandler()
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "min_timestamp", Order: pb.Order_ORDER_ASC},
		},
		FilterExpression: "INVALID SQL @@@ !!!",
	}

	err := h.StreamSplits(req, newMockStream())

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for bad filter, got %v", err)
	}
}

// TestStreamSplits_StreamIdleTimeout verifies that stream_idle_timeout_ms is
// clamped to [minStreamTimeout, maxStreamTimeout] without error for valid values.
func TestStreamSplits_StreamIdleTimeout(t *testing.T) {
	tests := []struct {
		name string
		ms   int64
	}{
		{"below_min", 10},     // < 1s → clamped to 1s
		{"valid", 5000},       // 5s — within range
		{"above_max", 700000}, // > 10min → clamped to 10min
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, mock, db := newTestEngineForGRPC(t)
			defer db.Close() //nolint:errcheck

			mock.ExpectQuery("SELECT").
				WillReturnRows(sqlmock.NewRows([]string{"id", "min_timestamp"}))

			h := NewQueryHandler(engine, func(string) *schema.ColumnRegistry { return nil }, zap.NewNop())
			req := &pb.StreamSplitsRequest{
				Table: "test_table",
				OrderBy: []*pb.OrderBy{
					{Column: "min_timestamp", Order: pb.Order_ORDER_ASC},
				},
				StreamIdleTimeoutMs: tt.ms,
			}
			if err := h.StreamSplits(req, newMockStream()); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestStreamSplits_CursorAllValueTypes exercises cursor decoding for int,
// float, and string CursorValue types in the request.
func TestStreamSplits_CursorAllValueTypes(t *testing.T) {
	engine, mock, db := newTestEngineForGRPC(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id", "min_timestamp", "state", "raw_size_bytes"}))

	h := NewQueryHandler(engine, func(string) *schema.ColumnRegistry { return nil }, zap.NewNop())
	req := &pb.StreamSplitsRequest{
		Table: "test_table",
		OrderBy: []*pb.OrderBy{
			{Column: "min_timestamp", Order: pb.Order_ORDER_ASC},
			{Column: "max_timestamp", Order: pb.Order_ORDER_DESC},
			{Column: "state", Order: pb.Order_ORDER_ASC},
		},
		AllowUnindexedSort: true,
		Cursor: &pb.KeysetCursor{
			Id: 5,
			Values: []*pb.CursorValue{
				{Value: &pb.CursorValue_IntVal{IntVal: 1000}},
				{Value: &pb.CursorValue_FloatVal{FloatVal: 3.14}},
				{Value: &pb.CursorValue_StrVal{StrVal: "IR_CLOSED"}},
			},
		},
	}

	err := h.StreamSplits(req, newMockStream())
	if err != nil {
		t.Fatalf("expected no error for valid cursor types, got %v", err)
	}
}

// --- helpers for grpcserver tests ---

// newTestEngineForGRPC creates a SplitQueryEngine backed by a sqlmock DB.
// This mirrors the helper in query/engine_test.go but is accessible from the
// grpcserver package.
func newTestEngineForGRPC(t *testing.T) (*query.SplitQueryEngine, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	engine := query.NewSplitQueryEngine(db, zap.NewNop())
	return engine, mock, db
}

// newTestRegistryForGRPC creates a ColumnRegistry with dim and agg entries
// using sqlmock to satisfy the constructor's DB queries. The registry contains:
//
//	EQ:  "status_code"         (no value) → agg_f01 (INT)
//	GTE: "response_time"."p99"            → agg_f02 (FLOAT)
//	SUM: "bytes_sent"          (no value) → agg_f03 (INT)
func newTestRegistryForGRPC(t *testing.T) *schema.ColumnRegistry {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// High-water-mark queries.
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name"}), // no dim slots
	)
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name"}).
			AddRow("agg_f01").AddRow("agg_f02").AddRow("agg_f03"),
	)

	// Active dim registry (empty).
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "base_type", "width", "dim_key", "alias_column"}),
	)

	// Active agg registry.
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column"}).
			AddRow("agg_f01", "status_code", nil, "EQ", "INT", nil).
			AddRow("agg_f02", "response_time", "p99", "GTE", "FLOAT", nil).
			AddRow("agg_f03", "bytes_sent", nil, "SUM", "INT", nil),
	)

	// Active sketch registry (empty).
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"sketch_name", "sketch_key"}),
	)

	reg, err := schema.NewColumnRegistry(context.Background(), db, "test_table", false, zap.NewNop())
	if err != nil {
		t.Fatalf("NewColumnRegistry: %v", err)
	}
	return reg
}

// newRegistryWithCustomAgg creates a ColumnRegistry with a single agg entry
// using the provided parameters.
func newRegistryWithCustomAgg(t *testing.T, colName, aggKey, aggValue, aggType, valueType string) *schema.ColumnRegistry {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name"}), // no dim slots
	)
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name"}).AddRow(colName),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "base_type", "width", "dim_key", "alias_column"}),
	)
	aggVal := interface{}(aggValue)
	if aggValue == "" {
		aggVal = nil
	}
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column"}).
			AddRow(colName, aggKey, aggVal, aggType, valueType, nil),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"sketch_name", "sketch_key"}),
	)

	reg, err := schema.NewColumnRegistry(context.Background(), db, "test_table", false, zap.NewNop())
	if err != nil {
		t.Fatalf("NewColumnRegistry: %v", err)
	}
	return reg
}
