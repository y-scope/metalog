package kafka

import (
	"testing"
)

func TestUnmarshalJSONToProto_MinimalPayload(t *testing.T) {
	payload := []byte(`{"state": "IR_BUFFERING", "min_timestamp": 500}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	if rec.File.State != "IR_BUFFERING" {
		t.Errorf("State = %q, want IR_BUFFERING", rec.File.State)
	}
	if rec.File.MinTimestamp != 500 {
		t.Errorf("MinTimestamp = %d, want 500", rec.File.MinTimestamp)
	}
}

func TestUnmarshalJSONToProto_InvalidJSON(t *testing.T) {
	_, err := unmarshalJSONToProto([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestUnmarshalJSONToProto_NoIR(t *testing.T) {
	payload := []byte(`{"state": "IR_CLOSED", "min_timestamp": 1000}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	if rec.File.Ir != nil {
		t.Error("IR should be nil when not provided")
	}
}

func TestUnmarshalJSONToProto_DimsDefaultWidth(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"dims": [{"key": "host", "value": "web-1", "width": 0}]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Dim) != 1 {
		t.Fatalf("expected 1 dim, got %d", len(rec.Dim))
	}
}

func TestUnmarshalJSONToProto_AggInvalidType(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"aggs": [{"field": "level", "type": "INVALID_TYPE", "int_val": 1}]
	}`)
	_, err := unmarshalJSONToProto(payload)
	if err == nil {
		t.Error("expected error for invalid agg type")
	}
}

func TestUnmarshalJSONToProto_AggDefaultType(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"aggs": [{"field": "count", "int_val": 42}]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Agg) != 1 {
		t.Fatalf("expected 1 agg, got %d", len(rec.Agg))
	}
}

func TestParseSelfDescribingEntry_NoSlash(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [{"key": "noslash", "value": "val"}]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	// Should pass through to SelfDescribingKv
	if len(rec.SelfDescribingKv) != 1 {
		t.Errorf("expected 1 pass-through entry, got %d", len(rec.SelfDescribingKv))
	}
}

func TestParseSelfDescribingEntry_EmptyAfterSlash(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [{"key": "prefix/", "value": "val"}]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	// Empty remainder after prefix/ -> pass through
	if len(rec.SelfDescribingKv) != 1 {
		t.Errorf("expected 1 pass-through entry, got %d", len(rec.SelfDescribingKv))
	}
}

func TestParseSelfDescribingDim_FloatType(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "dim/float/temperature", "value": "98.6"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Dim) != 1 {
		t.Fatalf("expected 1 dim, got %d", len(rec.Dim))
	}
	if rec.Dim[0].Key != "temperature" {
		t.Errorf("key = %q, want temperature", rec.Dim[0].Key)
	}
}

func TestParseSelfDescribingDim_BoolType(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "dim/bool/is_error", "value": "true"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Dim) != 1 {
		t.Fatalf("expected 1 dim, got %d", len(rec.Dim))
	}
}

func TestParseSelfDescribingDim_Utf8Type(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "dim/str128utf8/message", "value": "hello"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Dim) != 1 {
		t.Fatalf("expected 1 dim, got %d", len(rec.Dim))
	}
}

func TestParseSelfDescribingDim_MissingField(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "dim/str64", "value": "hello"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	// Missing field name -> error -> pass through to SelfDescribingKv
	if len(rec.SelfDescribingKv) != 1 {
		t.Errorf("expected 1 pass-through, got %d", len(rec.SelfDescribingKv))
	}
}

func TestParseSelfDescribingDim_DefaultWidth(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "dim/str/hostname", "value": "web-1"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Dim) != 1 {
		t.Fatalf("expected 1 dim, got %d", len(rec.Dim))
	}
}

func TestParseSelfDescribingAgg_WithQualifier(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "agg_int/gte/level/warn", "value": "42"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Agg) != 1 {
		t.Fatalf("expected 1 agg, got %d", len(rec.Agg))
	}
	if rec.Agg[0].Qualifier != "warn" {
		t.Errorf("qualifier = %q, want warn", rec.Agg[0].Qualifier)
	}
}

func TestParseSelfDescribingAgg_InvalidType(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "agg_int/INVALID/level", "value": "42"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	// Invalid agg type -> error -> pass through
	if len(rec.SelfDescribingKv) != 1 {
		t.Errorf("expected 1 pass-through, got %d", len(rec.SelfDescribingKv))
	}
}

func TestParseSelfDescribingAgg_MissingField(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "agg_int/gte", "value": "42"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	// Missing field -> error -> pass through
	if len(rec.SelfDescribingKv) != 1 {
		t.Errorf("expected 1 pass-through, got %d", len(rec.SelfDescribingKv))
	}
}

func TestParseSelfDescribingSketch_MissingField(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "sketch/parquet_sbbf", "value": "qg=="}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	// Missing field in sketch -> error -> pass through
	if len(rec.SelfDescribingKv) != 1 {
		t.Errorf("expected 1 pass-through, got %d", len(rec.SelfDescribingKv))
	}
}

func TestParseSelfDescribingSketch_InvalidBase64(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "sketch/sbbf/uuid", "value": "!!!invalid-base64!!!"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	// base64 decode error -> pass through
	if len(rec.SelfDescribingKv) != 1 {
		t.Errorf("expected 1 pass-through, got %d", len(rec.SelfDescribingKv))
	}
}

func TestNewTransformer_Known(t *testing.T) {
	names := []string{"", "auto", "proto"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			tr, err := NewTransformer(name)
			if err != nil {
				t.Fatalf("NewTransformer(%q) error: %v", name, err)
			}
			if tr == nil {
				t.Fatalf("NewTransformer(%q) returned nil", name)
			}
		})
	}
}

func TestNewTransformer_Unknown(t *testing.T) {
	_, err := NewTransformer("nonexistent_xyz")
	if err == nil {
		t.Error("expected error for unknown transformer")
	}
}

func TestProtoTransformer_InvalidPayload(t *testing.T) {
	tr := &ProtoTransformer{}
	_, err := tr.Transform([]byte("not protobuf"))
	if err == nil {
		t.Error("expected error for invalid protobuf")
	}
}

func TestAutoDetectTransformer_EmptyPayload(t *testing.T) {
	tr := &AutoDetectTransformer{}
	// Empty payload -> proto unmarshal of empty input (valid proto, empty record)
	// This will fail at ConvertRecord due to nil File
	_, err := tr.Transform([]byte{})
	if err == nil {
		t.Error("expected error for empty payload (nil File)")
	}
}

func TestParseSelfDescribingDim_IntInvalidValue(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "dim/int/code", "value": "not_a_number"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	// int parse error -> pass through
	if len(rec.SelfDescribingKv) != 1 {
		t.Errorf("expected 1 pass-through, got %d", len(rec.SelfDescribingKv))
	}
}

func TestParseSelfDescribingDim_FloatInvalidValue(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "dim/float/temp", "value": "not_a_float"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	// float parse error -> pass through
	if len(rec.SelfDescribingKv) != 1 {
		t.Errorf("expected 1 pass-through, got %d", len(rec.SelfDescribingKv))
	}
}

func TestParseSelfDescribingAgg_FloatInvalidValue(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "agg_float/avg/latency", "value": "not_a_float"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	// float parse error -> pass through
	if len(rec.SelfDescribingKv) != 1 {
		t.Errorf("expected 1 pass-through, got %d", len(rec.SelfDescribingKv))
	}
}

func TestParseSelfDescribingAgg_IntInvalidValue(t *testing.T) {
	payload := []byte(`{
		"state": "IR_CLOSED",
		"min_timestamp": 1000,
		"ir": {"path": "/test.ir"},
		"self_describing_kv": [
			{"key": "agg_int/gte/level", "value": "not_a_number"}
		]
	}`)
	rec, err := unmarshalJSONToProto(payload)
	if err != nil {
		t.Fatal(err)
	}
	// int parse error -> pass through
	if len(rec.SelfDescribingKv) != 1 {
		t.Errorf("expected 1 pass-through, got %d", len(rec.SelfDescribingKv))
	}
}

func TestParseWidthFromTypeSpec_Defaults(t *testing.T) {
	tests := []struct {
		spec string
		want int
	}{
		{"str128", 128},
		{"str64", 64},
		{"str", 64},    // no number -> default
		{"str0", 64},   // zero -> default
		{"str-5", 64},  // negative -> default
		{"128", 128},   // just number
		{"abc", 64},    // invalid -> default
	}
	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			got := parseWidthFromTypeSpec(tt.spec)
			if got != tt.want {
				t.Errorf("parseWidthFromTypeSpec(%q) = %d, want %d", tt.spec, got, tt.want)
			}
		})
	}
}
