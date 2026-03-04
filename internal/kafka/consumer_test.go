package kafka

import (
	"testing"
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

func TestAutoDetectTransformer_InvalidPayload(t *testing.T) {
	tr := &AutoDetectTransformer{}
	// Non-JSON, non-protobuf
	_, err := tr.Transform([]byte("not valid"))
	if err == nil {
		t.Fatal("Transform() should fail on invalid payload")
	}
}

func TestParseBootstrapServers(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"kafka:9092", 1},
		{"kafka1:9092,kafka2:9092,kafka3:9092", 3},
		{" kafka:9092 , kafka2:9092 ", 2},
		{"", 0},
		{",,,", 0},
	}

	for _, tt := range tests {
		result := ParseBootstrapServers(tt.input)
		if len(result) != tt.want {
			t.Errorf("ParseBootstrapServers(%q) = %d servers, want %d", tt.input, len(result), tt.want)
		}
	}
}

func TestValidateConfig_Valid(t *testing.T) {
	cfg := ConsumerConfig{
		BootstrapServers: "kafka:9092",
		Topic:            "test-topic",
		GroupID:          "test-group",
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Errorf("ValidateConfig() error = %v", err)
	}
}

func TestValidateConfig_MissingFields(t *testing.T) {
	tests := []struct {
		name string
		cfg  ConsumerConfig
	}{
		{"missing bootstrap", ConsumerConfig{Topic: "t", GroupID: "g"}},
		{"missing topic", ConsumerConfig{BootstrapServers: "kafka:9092", GroupID: "g"}},
		{"missing group", ConsumerConfig{BootstrapServers: "kafka:9092", Topic: "t"}},
	}

	for _, tt := range tests {
		if err := ValidateConfig(tt.cfg); err == nil {
			t.Errorf("ValidateConfig(%s) should fail", tt.name)
		}
	}
}

func TestBuildConfigMap(t *testing.T) {
	cfg := ConsumerConfig{
		BootstrapServers: "kafka:9092",
		GroupID:          "my-group",
		ExtraConfig: map[string]string{
			"session.timeout.ms": "30000",
		},
	}

	cm := BuildConfigMap(cfg)

	bs, err := cm.Get("bootstrap.servers", "")
	if err != nil {
		t.Fatal(err)
	}
	if bs != "kafka:9092" {
		t.Errorf("bootstrap.servers = %v, want kafka:9092", bs)
	}

	gid, err := cm.Get("group.id", "")
	if err != nil {
		t.Fatal(err)
	}
	if gid != "my-group" {
		t.Errorf("group.id = %v, want my-group", gid)
	}

	st, err := cm.Get("session.timeout.ms", "")
	if err != nil {
		t.Fatal(err)
	}
	if st != "30000" {
		t.Errorf("session.timeout.ms = %v, want 30000", st)
	}
}
