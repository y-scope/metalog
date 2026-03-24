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
	if result.MinTimestamp != 1000 {
		t.Errorf("MinTimestamp = %d, want 1000", result.MinTimestamp)
	}
	if result.MaxTimestamp != 2000 {
		t.Errorf("MaxTimestamp = %d, want 2000", result.MaxTimestamp)
	}
	if result.RecordCount != 42 {
		t.Errorf("RecordCount = %d, want 42", result.RecordCount)
	}
	if !result.ClpIRBucket.Valid || result.ClpIRBucket.String != "test-bucket" {
		t.Errorf("IR bucket not parsed correctly")
	}
	if _, ok := result.Dims["service"]; !ok {
		t.Errorf("Dims missing 'service' key")
	}
	if len(result.Aggs) != 1 {
		t.Errorf("expected 1 agg entry, got %d", len(result.Aggs))
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
	sketchData, ok := result.Sketches["uuid"]
	if !ok {
		t.Fatal("expected sketch entry for 'uuid'")
	}
	if len(sketchData) == 0 {
		t.Error("sketch data should not be empty")
	}

	// Verify the data is valid msgpack with type and data fields
	var snap struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	}
	if err := msgpack.Unmarshal(sketchData, &snap); err != nil {
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
	if len(result.Dims) != 2 {
		t.Fatalf("expected 2 dim entries, got %d", len(result.Dims))
	}
	if _, ok := result.Dims["service_name"]; !ok {
		t.Error("missing dim 'service_name'")
	}
	if _, ok := result.Dims["error_code"]; !ok {
		t.Error("missing dim 'error_code'")
	}
	if len(result.Aggs) != 2 {
		t.Fatalf("expected 2 agg entries, got %d", len(result.Aggs))
	}
	if len(result.AggMeta) != 2 {
		t.Fatalf("expected 2 agg meta entries, got %d", len(result.AggMeta))
	}
}

func TestAutoDetectTransformer_UnknownPrefixPassesThrough(t *testing.T) {
	// Unknown self-describing prefixes are kept in the proto SelfDescribingKv
	// but after ConvertRecord they are not in FileRecord (no mapping).
	// Just verify no error on transform.
	jsonPayload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/logs/test.clp.zst"},
		"self_describing_kv": [
			{"key": "custom/my_field", "value": "my_value"}
		]
	}`)

	tr := &AutoDetectTransformer{}
	_, err := tr.Transform(jsonPayload)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
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
