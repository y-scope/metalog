package grpc

import (
	"testing"

	"github.com/y-scope/metalog/internal/query"
)

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

	split := rowToProtoSplit(row)

	if split.Id != 42 {
		t.Errorf("Id = %d, want 42", split.Id)
	}
	if split.MinTimestamp != 1000 {
		t.Errorf("MinTimestamp = %d, want 1000", split.MinTimestamp)
	}
	if split.MaxTimestamp != 2000 {
		t.Errorf("MaxTimestamp = %d, want 2000", split.MaxTimestamp)
	}
	if split.State != "IR_CLOSED" {
		t.Errorf("State = %q, want IR_CLOSED", split.State)
	}
	if split.RecordCount != 500 {
		t.Errorf("RecordCount = %d, want 500", split.RecordCount)
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

	split := rowToProtoSplit(row)

	if split.ClpIrPath != "/logs/test.clp.zst" {
		t.Errorf("ClpIrPath = %q", split.ClpIrPath)
	}
	if split.ClpIrStorageBackend != "s3" {
		t.Errorf("ClpIrStorageBackend = %q", split.ClpIrStorageBackend)
	}
	if split.ClpIrBucket != "my-bucket" {
		t.Errorf("ClpIrBucket = %q", split.ClpIrBucket)
	}
	if split.SizeBytes != 4096 {
		t.Errorf("SizeBytes = %d, want 4096", split.SizeBytes)
	}
}

func TestRowToProtoSplit_ArchiveSizeOverridesIRSize(t *testing.T) {
	row := &query.SplitRow{
		ID: 1,
		Values: map[string]any{
			"clp_ir_size_bytes":      int64(1000),
			"clp_archive_size_bytes": int64(5000),
		},
	}

	split := rowToProtoSplit(row)

	if split.SizeBytes != 5000 {
		t.Errorf("SizeBytes = %d, want 5000 (archive should override IR)", split.SizeBytes)
	}
}

func TestRowToProtoSplit_ArchiveSizeZeroKeepsIRSize(t *testing.T) {
	row := &query.SplitRow{
		ID: 1,
		Values: map[string]any{
			"clp_ir_size_bytes":      int64(1000),
			"clp_archive_size_bytes": int64(0),
		},
	}

	split := rowToProtoSplit(row)

	if split.SizeBytes != 1000 {
		t.Errorf("SizeBytes = %d, want 1000 (zero archive should not override)", split.SizeBytes)
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

	split := rowToProtoSplit(row)

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

	split := rowToProtoSplit(row)

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

	split := rowToProtoSplit(row)

	// Wrong type should leave the field at zero value
	if split.MinTimestamp != 0 {
		t.Errorf("MinTimestamp = %d, want 0 for wrong type", split.MinTimestamp)
	}
}

func TestRowToProtoSplit_EmptyRow(t *testing.T) {
	row := &query.SplitRow{
		ID:     0,
		Values: map[string]any{},
	}

	split := rowToProtoSplit(row)

	if split.Id != 0 {
		t.Errorf("Id = %d, want 0", split.Id)
	}
	if len(split.Dimensions) != 0 {
		t.Errorf("Dimensions should be empty, got %d", len(split.Dimensions))
	}
}
