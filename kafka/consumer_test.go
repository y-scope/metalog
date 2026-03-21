package kafka

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestAutoDetectTransformer_ValidJSON(t *testing.T) {
	jsonPayload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"max_timestamp": 2000,
		"record_count": 42,
		"raw_size_bytes": 5000,
		"ir": {
			"storage_backend": "s3",
			"bucket": "test-bucket",
			"path": "/logs/test.clp.zst",
			"size_bytes": 1234
		},
		"dims": [
			{"key": "service", "value": "app-001"}
		],
		"aggs": [
			{"field": "level", "qualifier": "info", "type": "GTE", "int_val": 100}
		]
	}`)

	tr := &AutoDetectTransformer{}
	result, err := tr.Transform(jsonPayload)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if result.File.MinTimestamp != 1000 {
		t.Errorf("MinTimestamp = %d, want 1000", result.File.MinTimestamp)
	}
	if result.File.MaxTimestamp != 2000 {
		t.Errorf("MaxTimestamp = %d, want 2000", result.File.MaxTimestamp)
	}
	if result.File.RecordCount != 42 {
		t.Errorf("RecordCount = %d, want 42", result.File.RecordCount)
	}
	if result.File.Ir == nil || result.File.Ir.ClpIrBucket != "test-bucket" {
		t.Errorf("IR bucket not parsed correctly")
	}
	if len(result.Dim) != 1 || result.Dim[0].Key != "service" {
		t.Errorf("Dims not parsed correctly")
	}
	if len(result.Agg) != 1 || result.Agg[0].Field != "level" {
		t.Errorf("Aggs not parsed correctly")
	}
}

func TestAutoDetectTransformer_SelfDescribingSketch(t *testing.T) {
	// base64 of some arbitrary bytes (raw SBBF filter data)
	jsonPayload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/logs/test.clp.zst"},
		"self_describing_kv": [
			{"key": "sketch/parquet_sbbf_xxhash64/uuid", "value": "qg=="}
		]
	}`)

	tr := &AutoDetectTransformer{}
	result, err := tr.Transform(jsonPayload)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if len(result.Sketch) != 1 {
		t.Fatalf("expected 1 sketch entry, got %d", len(result.Sketch))
	}
	if result.Sketch[0].SketchKey != "uuid" {
		t.Errorf("SketchKey = %q, want uuid", result.Sketch[0].SketchKey)
	}
	if len(result.Sketch[0].Data) == 0 {
		t.Error("sketch data should not be empty")
	}

	// Verify the data is valid msgpack with type and data fields
	var snap struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	}
	if err := msgpack.Unmarshal(result.Sketch[0].Data, &snap); err != nil {
		t.Fatalf("unmarshal sketch data: %v", err)
	}
	if snap.Type != "parquet_sbbf_xxhash64" {
		t.Errorf("Type = %q, want parquet_sbbf_xxhash64", snap.Type)
	}
}

func TestAutoDetectTransformer_SelfDescribingDimAndAgg(t *testing.T) {
	jsonPayload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/logs/test.clp.zst"},
		"self_describing_kv": [
			{"key": "dim/str128/service_name", "value": "api-gw"},
			{"key": "dim/int/error_code", "value": "404"},
			{"key": "agg_int/gte/level/warn", "value": "42"},
			{"key": "agg_float/avg/latency", "value": "1.5"}
		]
	}`)

	tr := &AutoDetectTransformer{}
	result, err := tr.Transform(jsonPayload)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if len(result.Dim) != 2 {
		t.Fatalf("expected 2 dim entries, got %d", len(result.Dim))
	}
	if result.Dim[0].Key != "service_name" {
		t.Errorf("Dim[0].Key = %q, want service_name", result.Dim[0].Key)
	}
	if result.Dim[1].Key != "error_code" {
		t.Errorf("Dim[1].Key = %q, want error_code", result.Dim[1].Key)
	}
	if len(result.Agg) != 2 {
		t.Fatalf("expected 2 agg entries, got %d", len(result.Agg))
	}
	if result.Agg[0].Field != "level" || result.Agg[0].Qualifier != "warn" {
		t.Errorf("Agg[0] = %v/%v, want level/warn", result.Agg[0].Field, result.Agg[0].Qualifier)
	}
	if result.Agg[1].Field != "latency" {
		t.Errorf("Agg[1].Field = %q, want latency", result.Agg[1].Field)
	}
}

func TestAutoDetectTransformer_UnknownPrefixPassesThrough(t *testing.T) {
	jsonPayload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/logs/test.clp.zst"},
		"self_describing_kv": [
			{"key": "custom/my_field", "value": "my_value"}
		]
	}`)

	tr := &AutoDetectTransformer{}
	result, err := tr.Transform(jsonPayload)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if len(result.SelfDescribingKv) != 1 {
		t.Fatalf("expected 1 self-describing entry, got %d", len(result.SelfDescribingKv))
	}
	if result.SelfDescribingKv[0].Key != "custom/my_field" {
		t.Errorf("Key = %q, want custom/my_field", result.SelfDescribingKv[0].Key)
	}
}

func TestAutoDetectTransformer_InvalidPayload(t *testing.T) {
	tr := &AutoDetectTransformer{}
	// Non-JSON, non-protobuf
	_, err := tr.Transform([]byte("not valid"))
	if err == nil {
		t.Fatal("Transform() should fail on invalid payload")
	}
}

