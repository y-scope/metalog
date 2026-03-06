package kafka

import (
	"encoding/json"
	"testing"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
)

func TestObservabilityTransformer_BasicFields(t *testing.T) {
	payload := map[string]any{
		"tPath":            "/logs/app/test.clp.zst",
		"creationTime":     float64(1700000000000), // ms
		"lastModification": float64(1700000060000), // ms
		"bytesWritten":     float64(4096),
		"service_name":     "my-service",
		"hostname":         "host-1",
	}
	data, _ := json.Marshal(payload)

	tr := NewObservabilityTransformer("tb", 90, upDims)
	record, err := tr.Transform(data)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	if record.File.State != "IR_ARCHIVE_BUFFERING" {
		t.Errorf("State = %q, want IR_ARCHIVE_BUFFERING", record.File.State)
	}
	if record.File.MinTimestamp != 1700000000000*1_000_000 {
		t.Errorf("MinTimestamp = %d, want %d", record.File.MinTimestamp, 1700000000000*1_000_000)
	}
	if record.File.MaxTimestamp != 1700000060000*1_000_000 {
		t.Errorf("MaxTimestamp = %d, want %d", record.File.MaxTimestamp, 1700000060000*1_000_000)
	}
	if record.File.Ir.ClpIrPath != "logs/app/test.clp.zst" {
		t.Errorf("ClpIrPath = %q, want leading slash stripped", record.File.Ir.ClpIrPath)
	}
	if record.File.Ir.ClpIrSizeBytes != 4096 {
		t.Errorf("ClpIrSizeBytes = %d, want 4096", record.File.Ir.ClpIrSizeBytes)
	}
	if record.File.Ir.ClpIrStorageBackend != "tb" {
		t.Errorf("ClpIrStorageBackend = %q, want tb", record.File.Ir.ClpIrStorageBackend)
	}
	if record.File.RetentionDays != 90 {
		t.Errorf("RetentionDays = %d, want 90", record.File.RetentionDays)
	}
}

func TestObservabilityTransformer_Dimensions(t *testing.T) {
	payload := map[string]any{
		"tPath":        "/test.clp",
		"creationTime": float64(1000),
		"service_name": "svc",
		"instance":     "i-123",
		"region":       "us-east-1",
	}
	data, _ := json.Marshal(payload)

	tr := NewObservabilityTransformer("tb", 30, upDims)
	record, err := tr.Transform(data)
	if err != nil {
		t.Fatal(err)
	}

	// Should have 3 dims (service_name, instance, region) — others missing
	if len(record.Dim) != 3 {
		t.Fatalf("expected 3 dims, got %d", len(record.Dim))
	}

	dimMap := make(map[string]*pb.DimEntry)
	for _, d := range record.Dim {
		dimMap[d.Key] = d
	}

	sn := dimMap["service_name"]
	if sn == nil {
		t.Fatal("missing service_name dim")
	}
	strDim := sn.Value.GetStr()
	if strDim == nil || strDim.Value != "svc" || strDim.MaxLength != 64 {
		t.Errorf("service_name dim = %+v", sn.Value)
	}
}

func TestObservabilityTransformer_IntDimension(t *testing.T) {
	payload := map[string]any{
		"tPath":              "/test.clp",
		"creationTime":       float64(1000),
		"spark_app_id":       "app-123",
		"spark_app_attempt_id": float64(3),
	}
	data, _ := json.Marshal(payload)

	tr := NewObservabilityTransformer("tb", 30, sparkDims)
	record, err := tr.Transform(data)
	if err != nil {
		t.Fatal(err)
	}

	dimMap := make(map[string]*pb.DimEntry)
	for _, d := range record.Dim {
		dimMap[d.Key] = d
	}

	attempt := dimMap["spark_app_attempt_id"]
	if attempt == nil {
		t.Fatal("missing spark_app_attempt_id dim")
	}
	if attempt.Value.GetIntVal() != 3 {
		t.Errorf("spark_app_attempt_id = %d, want 3", attempt.Value.GetIntVal())
	}
}

func TestObservabilityTransformer_FallbackMaxTimestamp(t *testing.T) {
	payload := map[string]any{
		"tPath":        "/test.clp",
		"creationTime": float64(5000),
		// no lastModification
	}
	data, _ := json.Marshal(payload)

	tr := NewObservabilityTransformer("tb", 0, nil)
	record, err := tr.Transform(data)
	if err != nil {
		t.Fatal(err)
	}

	// maxTimestamp should fall back to creationTime
	if record.File.MaxTimestamp != 5000*1_000_000 {
		t.Errorf("MaxTimestamp = %d, want %d (fallback to creationTime)", record.File.MaxTimestamp, 5000*1_000_000)
	}
}

func TestObservabilityTransformer_CaseInsensitive(t *testing.T) {
	payload := map[string]any{
		"TPath":        "/test.clp",
		"CreationTime": float64(1000),
		"SERVICE_NAME": "upper",
	}
	data, _ := json.Marshal(payload)

	tr := NewObservabilityTransformer("tb", 30, upDims)
	record, err := tr.Transform(data)
	if err != nil {
		t.Fatal(err)
	}

	if record.File.Ir.ClpIrPath != "test.clp" {
		t.Errorf("ClpIrPath = %q, want case-insensitive match", record.File.Ir.ClpIrPath)
	}

	// service_name should match SERVICE_NAME
	found := false
	for _, d := range record.Dim {
		if d.Key == "service_name" {
			found = true
			if d.Value.GetStr().Value != "upper" {
				t.Errorf("service_name = %q, want upper", d.Value.GetStr().Value)
			}
		}
	}
	if !found {
		t.Error("service_name dim not found (case-insensitive lookup failed)")
	}
}

func TestNewTransformer_Observability(t *testing.T) {
	platforms := []string{"up", "ray", "warm-storage", "athena", "spark"}
	for _, name := range platforms {
		tr := NewTransformer(name)
		if _, ok := tr.(*ObservabilityTransformer); !ok {
			t.Errorf("NewTransformer(%q) = %T, want *ObservabilityTransformer", name, tr)
		}
	}
}

func TestNewTransformer_Fallback(t *testing.T) {
	tr := NewTransformer("nonexistent")
	if _, ok := tr.(*AutoDetectTransformer); !ok {
		t.Errorf("NewTransformer(nonexistent) = %T, want *AutoDetectTransformer fallback", tr)
	}
}
