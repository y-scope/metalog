package ingestion

import (
	"context"
	"testing"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/metastore"
	"go.uber.org/zap"
)

func TestExtractSketches_PopulatesFields(t *testing.T) {
	rec := &metastore.FileRecord{
		Sketches: make(map[string][]byte),
	}

	sketches := []*pb.SketchEntry{
		{SketchKey: "uuid", Data: []byte{1, 2, 3}},
		{SketchKey: "trace_id", Data: []byte{4, 5, 6}},
	}

	extractSketches(sketches, rec)

	if len(rec.Sketches) != 2 {
		t.Fatalf("expected 2 sketches, got %d", len(rec.Sketches))
	}
	if string(rec.Sketches["uuid"]) != string([]byte{1, 2, 3}) {
		t.Error("uuid sketch data mismatch")
	}
	if string(rec.Sketches["trace_id"]) != string([]byte{4, 5, 6}) {
		t.Error("trace_id sketch data mismatch")
	}
}

func TestExtractSketches_SkipsEmptyKeyAndData(t *testing.T) {
	rec := &metastore.FileRecord{
		Sketches: make(map[string][]byte),
	}

	sketches := []*pb.SketchEntry{
		{SketchKey: "", Data: []byte{1, 2, 3}},  // empty key
		{SketchKey: "uuid", Data: nil},           // nil data
		{SketchKey: "uuid", Data: []byte{}},      // empty data
		{SketchKey: "valid", Data: []byte{1}},    // valid
	}

	extractSketches(sketches, rec)

	if len(rec.Sketches) != 1 {
		t.Fatalf("expected 1 sketch, got %d", len(rec.Sketches))
	}
	if _, ok := rec.Sketches["valid"]; !ok {
		t.Error("expected 'valid' sketch to be present")
	}
}

// newTestWriter creates a BatchingWriter backed by a mock flusher for unit tests.
func newTestWriter(t *testing.T) *BatchingWriter {
	t.Helper()
	bw := NewBatchingWriter(context.Background(), nil, true, zap.NewNop(),
		WithBatchSize(100),
	)
	bw.testFlusher = &mockFlusher{}
	t.Cleanup(bw.Stop)
	return bw
}

func TestNewService(t *testing.T) {
	bw := newTestWriter(t)
	s := NewService(bw, true, zap.NewNop())
	if s == nil {
		t.Fatal("NewService returned nil")
	}
	if s.writer != bw {
		t.Error("writer not set")
	}
	if !s.blocking {
		t.Error("blocking should be true")
	}
}

func TestValidationError_Error(t *testing.T) {
	err := &ValidationError{Msg: "table_name is required"}
	if err.Error() != "table_name is required" {
		t.Errorf("Error() = %q, want %q", err.Error(), "table_name is required")
	}
}

func TestIngest_EmptyTableName(t *testing.T) {
	bw := newTestWriter(t)
	s := NewService(bw, false, zap.NewNop())
	rec := &metastore.FileRecord{}
	result := s.Ingest(context.Background(), "", rec)
	if result.Accepted {
		t.Error("Ingest should reject empty table name")
	}
	if result.Err == nil {
		t.Error("expected error for empty table name")
	}
}

func TestIngest_NilRecord(t *testing.T) {
	bw := newTestWriter(t)
	s := NewService(bw, false, zap.NewNop())
	result := s.Ingest(context.Background(), "my_table", nil)
	if result.Accepted {
		t.Error("Ingest should reject nil record")
	}
	if result.Err == nil {
		t.Error("expected error for nil record")
	}
}

func TestIngest_Accepted(t *testing.T) {
	bw := newTestWriter(t)
	s := NewService(bw, false, zap.NewNop())
	rec := &metastore.FileRecord{}
	result := s.Ingest(context.Background(), "my_table", rec)
	if !result.Accepted {
		t.Errorf("Ingest failed: %v", result.Err)
	}
}

func TestIngestWithCallback_EmptyTableName(t *testing.T) {
	bw := newTestWriter(t)
	s := NewService(bw, false, zap.NewNop())
	err := s.IngestWithCallback(context.Background(), "", &metastore.FileRecord{}, nil)
	if err == nil {
		t.Error("expected error for empty table name")
	}
}

func TestIngestWithCallback_NilRecord(t *testing.T) {
	bw := newTestWriter(t)
	s := NewService(bw, false, zap.NewNop())
	err := s.IngestWithCallback(context.Background(), "my_table", nil, nil)
	if err == nil {
		t.Error("expected error for nil record")
	}
}

func TestIngestWithCallback_SetsFlushedChan(t *testing.T) {
	bw := newTestWriter(t)
	s := NewService(bw, false, zap.NewNop())
	flushed := make(chan error, 1)
	rec := &metastore.FileRecord{}
	if err := s.IngestWithCallback(context.Background(), "my_table", rec, flushed); err != nil {
		t.Fatalf("IngestWithCallback error: %v", err)
	}
	if rec.Flushed != flushed {
		t.Error("rec.Flushed not set to provided channel")
	}
}


func TestNewDefaultTransformer(t *testing.T) {
	tr := NewDefaultTransformer()
	if tr == nil {
		t.Fatal("NewDefaultTransformer returned nil")
	}
}

func TestDefaultTransformer_Transform(t *testing.T) {
	tr := NewDefaultTransformer()
	rec := &metastore.FileRecord{}
	data := map[string]string{"host": "web-1", "env": "prod"}
	if err := tr.Transform(rec, data); err != nil {
		t.Fatal(err)
	}
	if rec.Dims["host"] != "web-1" {
		t.Errorf("Dims[host] = %v, want web-1", rec.Dims["host"])
	}
	if rec.Dims["env"] != "prod" {
		t.Errorf("Dims[env] = %v, want prod", rec.Dims["env"])
	}
}

func TestDefaultTransformer_Transform_InitializesMaps(t *testing.T) {
	tr := NewDefaultTransformer()
	rec := &metastore.FileRecord{} // Dims and Aggs both nil
	if err := tr.Transform(rec, map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	if rec.Dims == nil {
		t.Error("Dims should be initialized after Transform")
	}
	if rec.Aggs == nil {
		t.Error("Aggs should be initialized after Transform")
	}
}

func TestNewRecordTransformer_Default(t *testing.T) {
	tr, err := NewRecordTransformer("default")
	if err != nil {
		t.Fatalf("NewRecordTransformer(\"default\") error: %v", err)
	}
	if tr == nil {
		t.Fatal("returned nil transformer")
	}
}

func TestNewRecordTransformer_Empty(t *testing.T) {
	tr, err := NewRecordTransformer("")
	if err != nil {
		t.Fatalf("NewRecordTransformer(\"\") error: %v", err)
	}
	if tr == nil {
		t.Fatal("returned nil transformer")
	}
}

func TestNewRecordTransformer_Unknown(t *testing.T) {
	_, err := NewRecordTransformer("no_such_transformer")
	if err == nil {
		t.Error("expected error for unknown transformer name")
	}
}

func TestRegisterRecordTransformer_CustomName(t *testing.T) {
	RegisterRecordTransformer("test_custom", func() RecordTransformer {
		return NewDefaultTransformer()
	})
	tr, err := NewRecordTransformer("test_custom")
	if err != nil {
		t.Fatalf("NewRecordTransformer(\"test_custom\") error: %v", err)
	}
	if tr == nil {
		t.Fatal("returned nil transformer")
	}
}
