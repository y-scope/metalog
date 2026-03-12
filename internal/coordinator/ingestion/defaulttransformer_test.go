package ingestion

import (
	"testing"

	"github.com/y-scope/metalog/internal/metastore"
)

func TestJsonRecordTransformer_FlatData(t *testing.T) {
	tr := newJSONRecordTransformer("")
	rec := &metastore.FileRecord{}
	data := map[string]string{"host": "web-1", "level": "ERROR"}

	if err := tr.Transform(rec, data); err != nil {
		t.Fatal(err)
	}
	if rec.Dims["host"] != "web-1" {
		t.Errorf("Dims[host] = %v, want web-1", rec.Dims["host"])
	}
}

func TestJsonRecordTransformer_NestedJSON(t *testing.T) {
	tr := newJSONRecordTransformer("payload")
	rec := &metastore.FileRecord{}
	data := map[string]string{
		"payload": `{"app": "web", "meta": {"trace_id": "xyz"}, "count": 42}`,
	}

	if err := tr.Transform(rec, data); err != nil {
		t.Fatal(err)
	}

	if rec.Dims["app"] != "web" {
		t.Errorf("Dims[app] = %v, want web", rec.Dims["app"])
	}
	if rec.Dims["meta.trace_id"] != "xyz" {
		t.Errorf("Dims[meta.trace_id] = %v, want xyz", rec.Dims["meta.trace_id"])
	}
	if rec.Dims["count"] != "42" {
		t.Errorf("Dims[count] = %v, want 42", rec.Dims["count"])
	}
}

func TestJsonRecordTransformer_InvalidJSON(t *testing.T) {
	tr := newJSONRecordTransformer("payload")
	rec := &metastore.FileRecord{}
	data := map[string]string{"payload": "not json"}

	if err := tr.Transform(rec, data); err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestJsonRecordTransformer_MissingPayloadKey(t *testing.T) {
	tr := newJSONRecordTransformer("payload")
	rec := &metastore.FileRecord{}
	data := map[string]string{"other": "value"}

	// Should not error — just no-op
	if err := tr.Transform(rec, data); err != nil {
		t.Fatal(err)
	}
	if len(rec.Dims) != 0 {
		t.Errorf("Dims should be empty, got %v", rec.Dims)
	}
}

func TestJsonRecordTransformer_ArrayField(t *testing.T) {
	tr := newJSONRecordTransformer("payload")
	rec := &metastore.FileRecord{}
	data := map[string]string{
		"payload": `{"tags": ["a", "b", "c"]}`,
	}

	if err := tr.Transform(rec, data); err != nil {
		t.Fatal(err)
	}
	if rec.Dims["tags"] != `["a","b","c"]` {
		t.Errorf("Dims[tags] = %v, want JSON array string", rec.Dims["tags"])
	}
}

func TestJsonTransformerInRegistry(t *testing.T) {
	factory, ok := TransformerRegistry["json"]
	if !ok {
		t.Fatal("json transformer not registered")
	}
	tr := factory()
	if tr == nil {
		t.Fatal("factory returned nil")
	}
}
