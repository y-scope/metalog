package grpcserver

import (
	"context"
	"testing"

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

// --- toCursorValue tests ---

func TestToCursorValue_Nil(t *testing.T) {
	cv := toCursorValue(nil)
	if cv.GetValue() != nil {
		t.Errorf("nil should produce empty CursorValue, got %v", cv.GetValue())
	}
}

func TestToCursorValue_Int64(t *testing.T) {
	cv := toCursorValue(int64(42))
	if cv.GetIntVal() != 42 {
		t.Errorf("IntVal = %d, want 42", cv.GetIntVal())
	}
}

func TestToCursorValue_Int32(t *testing.T) {
	cv := toCursorValue(int32(7))
	if cv.GetIntVal() != 7 {
		t.Errorf("IntVal = %d, want 7", cv.GetIntVal())
	}
}

func TestToCursorValue_Uint64(t *testing.T) {
	cv := toCursorValue(uint64(100))
	if cv.GetIntVal() != 100 {
		t.Errorf("IntVal = %d, want 100", cv.GetIntVal())
	}
}

func TestToCursorValue_Uint32(t *testing.T) {
	cv := toCursorValue(uint32(50))
	if cv.GetIntVal() != 50 {
		t.Errorf("IntVal = %d, want 50", cv.GetIntVal())
	}
}

func TestToCursorValue_Float64(t *testing.T) {
	cv := toCursorValue(float64(3.14))
	if cv.GetFloatVal() != 3.14 {
		t.Errorf("FloatVal = %f, want 3.14", cv.GetFloatVal())
	}
}

func TestToCursorValue_Float32(t *testing.T) {
	cv := toCursorValue(float32(2.5))
	if cv.GetFloatVal() != 2.5 {
		t.Errorf("FloatVal = %f, want 2.5", cv.GetFloatVal())
	}
}

func TestToCursorValue_String(t *testing.T) {
	cv := toCursorValue("hello")
	if cv.GetStrVal() != "hello" {
		t.Errorf("StrVal = %q, want hello", cv.GetStrVal())
	}
}

func TestToCursorValue_Bytes(t *testing.T) {
	cv := toCursorValue([]byte("raw"))
	if cv.GetStrVal() != "raw" {
		t.Errorf("StrVal = %q, want raw", cv.GetStrVal())
	}
}

func TestToCursorValue_FallbackToString(t *testing.T) {
	cv := toCursorValue(true) // bool → fmt.Sprintf
	if cv.GetStrVal() != "true" {
		t.Errorf("StrVal = %q, want true", cv.GetStrVal())
	}
}

// --- dbValToInt64 tests ---

func TestDbValToInt64_Int64(t *testing.T) {
	v, ok := query.DBValToInt64(int64(99))
	if !ok || v != 99 {
		t.Errorf("got (%d, %v), want (99, true)", v, ok)
	}
}

func TestDbValToInt64_Int32(t *testing.T) {
	v, ok := query.DBValToInt64(int32(7))
	if !ok || v != 7 {
		t.Errorf("got (%d, %v), want (7, true)", v, ok)
	}
}

func TestDbValToInt64_Float64(t *testing.T) {
	v, ok := query.DBValToInt64(float64(42.0))
	if !ok || v != 42 {
		t.Errorf("got (%d, %v), want (42, true)", v, ok)
	}
}

func TestDbValToInt64_Unsupported(t *testing.T) {
	v, ok := query.DBValToInt64("not-a-number")
	if ok || v != 0 {
		t.Errorf("got (%d, %v), want (0, false)", v, ok)
	}
}

// --- dbValToFloat64 tests ---

func TestDbValToFloat64_Float64(t *testing.T) {
	v, ok := query.DBValToFloat64(float64(3.14))
	if !ok || v != 3.14 {
		t.Errorf("got (%f, %v), want (3.14, true)", v, ok)
	}
}

func TestDbValToFloat64_Int64(t *testing.T) {
	v, ok := query.DBValToFloat64(int64(10))
	if !ok || v != 10.0 {
		t.Errorf("got (%f, %v), want (10.0, true)", v, ok)
	}
}

func TestDbValToFloat64_Int32(t *testing.T) {
	v, ok := query.DBValToFloat64(int32(5))
	if !ok || v != 5.0 {
		t.Errorf("got (%f, %v), want (5.0, true)", v, ok)
	}
}

func TestDbValToFloat64_Unsupported(t *testing.T) {
	v, ok := query.DBValToFloat64("nope")
	if ok || v != 0 {
		t.Errorf("got (%f, %v), want (0, false)", v, ok)
	}
}

// --- dbValToString tests ---

func TestDbValToString(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want string
	}{
		{"string", "hello", "hello"},
		{"bytes", []byte("raw"), "raw"},
		{"nil", nil, ""},
		{"int", 42, "42"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := query.DBValToString(tt.val)
			if got != tt.want {
				t.Errorf("dbValToString(%v) = %q, want %q", tt.val, got, tt.want)
			}
		})
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
