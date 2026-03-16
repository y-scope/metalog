package ingestion

import (
	"testing"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/internal/metastore"
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
		{SketchKey: "", Data: []byte{1, 2, 3}},   // empty key
		{SketchKey: "uuid", Data: nil},             // nil data
		{SketchKey: "uuid", Data: []byte{}},        // empty data
		{SketchKey: "valid", Data: []byte{1}},      // valid
	}

	extractSketches(sketches, rec)

	if len(rec.Sketches) != 1 {
		t.Fatalf("expected 1 sketch, got %d", len(rec.Sketches))
	}
	if _, ok := rec.Sketches["valid"]; !ok {
		t.Error("expected 'valid' sketch to be present")
	}
}
