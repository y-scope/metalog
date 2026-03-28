package ingestion

import (
	"testing"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/metastore"
)

func TestConvertRecord_NilInput(t *testing.T) {
	_, err := ConvertRecord(nil)
	if err == nil {
		t.Error("ConvertRecord(nil) should return error")
	}
}

func TestConvertRecord_NilFile(t *testing.T) {
	record := &pb.MetadataRecord{File: nil}
	_, err := ConvertRecord(record)
	if err == nil {
		t.Error("ConvertRecord with nil File should return error")
	}
}

func TestFileRecordFromProto_BasicFields(t *testing.T) {
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State:         "IR_BUFFERING",
			MinTimestamp:  1000000000,
			MaxTimestamp:  2000000000,
			RawSizeBytes:  1024,
			RecordCount:   100,
			RetentionDays: 30,
			ExpiresAt:     3000000000,
		},
	}

	result, _ := fileRecordFromProto(record)

	if result == nil {
		t.Fatal("fileRecordFromProto returned nil")
	}

	if result.State != metastore.FileState("IR_BUFFERING") {
		t.Errorf("State = %q, want %q", result.State, "IR_BUFFERING")
	}
	if result.MinTimestamp != 1000000000 {
		t.Errorf("MinTimestamp = %d, want %d", result.MinTimestamp, 1000000000)
	}
	if result.MaxTimestamp != 2000000000 {
		t.Errorf("MaxTimestamp = %d, want %d", result.MaxTimestamp, 2000000000)
	}
	if !result.RawSizeBytes.Valid || result.RawSizeBytes.Int64 != 1024 {
		t.Errorf("RawSizeBytes = %v, want Valid=true, Int64=1024", result.RawSizeBytes)
	}
	if result.RecordCount != 100 {
		t.Errorf("RecordCount = %d, want %d", result.RecordCount, 100)
	}
	if result.RetentionDays != 30 {
		t.Errorf("RetentionDays = %d, want %d", result.RetentionDays, 30)
	}
	if result.ExpiresAt != 3000000000 {
		t.Errorf("ExpiresAt = %d, want %d", result.ExpiresAt, 3000000000)
	}
}

func TestFileRecordFromProto_IRFields(t *testing.T) {
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State: "IR_BUFFERING",
			Ir: &pb.IrFileInfo{
				ClpIrStorageBackend: "s3",
				ClpIrBucket:         "logs-bucket",
				ClpIrPath:           "/path/to/file.ir",
				ClpIrSizeBytes:      2048,
			},
		},
	}

	result, _ := fileRecordFromProto(record)

	if !result.ClpIRStorageBackend.Valid || result.ClpIRStorageBackend.String != "s3" {
		t.Errorf("ClpIRStorageBackend = %v, want s3", result.ClpIRStorageBackend)
	}
	if !result.ClpIRBucket.Valid || result.ClpIRBucket.String != "logs-bucket" {
		t.Errorf("ClpIRBucket = %v, want logs-bucket", result.ClpIRBucket)
	}
	if !result.ClpIRPath.Valid || result.ClpIRPath.String != "/path/to/file.ir" {
		t.Errorf("ClpIRPath = %v, want /path/to/file.ir", result.ClpIRPath)
	}
	if !result.ClpIRSizeBytes.Valid || result.ClpIRSizeBytes.Int64 != 2048 {
		t.Errorf("ClpIRSizeBytes = %v, want 2048", result.ClpIRSizeBytes)
	}
}

func TestFileRecordFromProto_ArchiveFields(t *testing.T) {
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State: "ARCHIVE_CLOSED",
			Archive: &pb.ArchiveFileInfo{
				ClpArchiveStorageBackend: "gcs",
				ClpArchiveBucket:         "archive-bucket",
				ClpArchivePath:           "/path/to/archive.tar",
				ClpArchiveCreatedAt:      1704067200000000000,
				ClpArchiveSizeBytes:      4096,
			},
		},
	}

	result, _ := fileRecordFromProto(record)

	if !result.ClpArchiveStorageBackend.Valid || result.ClpArchiveStorageBackend.String != "gcs" {
		t.Errorf("ClpArchiveStorageBackend = %v, want gcs", result.ClpArchiveStorageBackend)
	}
	if !result.ClpArchiveBucket.Valid || result.ClpArchiveBucket.String != "archive-bucket" {
		t.Errorf("ClpArchiveBucket = %v, want archive-bucket", result.ClpArchiveBucket)
	}
	if !result.ClpArchivePath.Valid || result.ClpArchivePath.String != "/path/to/archive.tar" {
		t.Errorf("ClpArchivePath = %v, want /path/to/archive.tar", result.ClpArchivePath)
	}
	if result.ClpArchiveCreatedAt != 1704067200000000000 {
		t.Errorf("ClpArchiveCreatedAt = %d, want 1704067200000000000", result.ClpArchiveCreatedAt)
	}
	if !result.ClpArchiveSizeBytes.Valid || result.ClpArchiveSizeBytes.Int64 != 4096 {
		t.Errorf("ClpArchiveSizeBytes = %v, want 4096", result.ClpArchiveSizeBytes)
	}
}

func TestFileRecordFromProto_NoArchive(t *testing.T) {
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State:   "IR_BUFFERING",
			Archive: nil,
		},
	}

	result, _ := fileRecordFromProto(record)

	// Archive fields should be null/invalid
	if result.ClpArchiveStorageBackend.Valid {
		t.Error("ClpArchiveStorageBackend should be invalid when no archive")
	}
	if result.ClpArchiveBucket.Valid {
		t.Error("ClpArchiveBucket should be invalid when no archive")
	}
	if result.ClpArchivePath.Valid {
		t.Error("ClpArchivePath should be invalid when no archive")
	}
	if result.ClpArchiveSizeBytes.Valid {
		t.Error("ClpArchiveSizeBytes should be invalid when no archive")
	}
}

func TestFileRecordFromProto_DimsAndAggsInitialized(t *testing.T) {
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State: "IR_BUFFERING",
		},
	}

	result, _ := fileRecordFromProto(record)

	if result.Dims == nil {
		t.Error("Dims should be initialized (not nil)")
	}
	if result.Aggs == nil {
		t.Error("Aggs should be initialized (not nil)")
	}
	if len(result.Dims) != 0 {
		t.Errorf("Dims should be empty, got %d entries", len(result.Dims))
	}
	if len(result.Aggs) != 0 {
		t.Errorf("Aggs should be empty, got %d entries", len(result.Aggs))
	}
}

func TestFileRecordFromProto_ExpiresAtComputed(t *testing.T) {
	// When ExpiresAt is 0 and MinTimestamp is set, compute from retention_days.
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State:         "IR_BUFFERING",
			MinTimestamp:  1704067200000000000, // 2024-01-01 00:00:00 UTC
			RetentionDays: 30,
			ExpiresAt:     0,
		},
	}

	result, _ := fileRecordFromProto(record)

	want := int64(1704067200000000000) + 30*86400*1e9
	if result.ExpiresAt != want {
		t.Errorf("ExpiresAt = %d, want %d (computed from min_timestamp + 30 days)", result.ExpiresAt, want)
	}
}

func TestFileRecordFromProto_ExpiresAtExplicit(t *testing.T) {
	// When ExpiresAt is explicitly provided, use it as-is.
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State:         "IR_BUFFERING",
			MinTimestamp:  1704067200000000000,
			RetentionDays: 30,
			ExpiresAt:     3000000000000000000,
		},
	}

	result, _ := fileRecordFromProto(record)

	if result.ExpiresAt != 3000000000000000000 {
		t.Errorf("ExpiresAt = %d, want 3000000000000000000 (explicit value preserved)", result.ExpiresAt)
	}
}

func TestFileRecordFromProto_DefaultRetentionDays(t *testing.T) {
	// When RetentionDays is 0, use the default (30 days).
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State:         "IR_BUFFERING",
			MinTimestamp:  1704067200000000000,
			RetentionDays: 0,
			ExpiresAt:     0,
		},
	}

	result, _ := fileRecordFromProto(record)

	if result.RetentionDays != metastore.DefaultRetentionDays {
		t.Errorf("RetentionDays = %d, want %d", result.RetentionDays, metastore.DefaultRetentionDays)
	}
	want := int64(1704067200000000000) + int64(metastore.DefaultRetentionDays)*86400*1e9
	if result.ExpiresAt != want {
		t.Errorf("ExpiresAt = %d, want %d (computed from default retention)", result.ExpiresAt, want)
	}
}

func TestConvertRecord_WithDimsAndSketches(t *testing.T) {
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State:        "IR_BUFFERING",
			MinTimestamp: 1000000000,
			Ir: &pb.IrFileInfo{
				ClpIrPath: "/path/to/file.ir",
			},
		},
		Sketch: []*pb.SketchEntry{
			{SketchKey: "uuid", Data: []byte{1, 2, 3}},
		},
	}

	rec, err := ConvertRecord(record)
	if err != nil {
		t.Fatal(err)
	}

	if len(rec.Sketches) != 1 {
		t.Fatalf("expected 1 sketch, got %d", len(rec.Sketches))
	}
	if string(rec.Sketches["uuid"]) != string([]byte{1, 2, 3}) {
		t.Error("uuid sketch data mismatch")
	}
}

func TestExtractDims_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		entry    *pb.DimEntry
		wantVal  any
		wantType string
		wantKey  string
	}{
		{
			name: "str value",
			entry: &pb.DimEntry{
				Key: "host",
				Value: &pb.DimensionValue{
					Value: &pb.DimensionValue_Str{
						Str: &pb.StringDimension{Value: "web-1", MaxLength: 64},
					},
				},
			},
			wantKey:  "host",
			wantVal:  "web-1",
			wantType: "str",
		},
		{
			name: "str_utf8 value",
			entry: &pb.DimEntry{
				Key: "message",
				Value: &pb.DimensionValue{
					Value: &pb.DimensionValue_StrUtf8{
						StrUtf8: &pb.StringDimension{Value: "hello", MaxLength: 128},
					},
				},
			},
			wantKey:  "message",
			wantVal:  "hello",
			wantType: "str_utf8",
		},
		{
			name: "int value",
			entry: &pb.DimEntry{
				Key: "status_code",
				Value: &pb.DimensionValue{
					Value: &pb.DimensionValue_IntVal{IntVal: 200},
				},
			},
			wantKey:  "status_code",
			wantVal:  int64(200),
			wantType: "int",
		},
		{
			name: "bool value",
			entry: &pb.DimEntry{
				Key: "is_error",
				Value: &pb.DimensionValue{
					Value: &pb.DimensionValue_BoolVal{BoolVal: true},
				},
			},
			wantKey:  "is_error",
			wantVal:  true,
			wantType: "bool",
		},
		{
			name: "float value",
			entry: &pb.DimEntry{
				Key: "latency",
				Value: &pb.DimensionValue{
					Value: &pb.DimensionValue_FloatVal{FloatVal: 3.14},
				},
			},
			wantKey:  "latency",
			wantVal:  3.14,
			wantType: "float",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &metastore.FileRecord{
				Dims: make(map[string]any),
				Aggs: make(map[string]any),
			}
			extractDims([]*pb.DimEntry{tt.entry}, rec)

			got, ok := rec.Dims[tt.wantKey]
			if !ok {
				t.Fatalf("Dims[%q] not set", tt.wantKey)
			}
			if got != tt.wantVal {
				t.Errorf("Dims[%q] = %v, want %v", tt.wantKey, got, tt.wantVal)
			}
			if len(rec.DimMeta) != 1 {
				t.Fatalf("DimMeta len = %d, want 1", len(rec.DimMeta))
			}
			if rec.DimMeta[0].BaseType != tt.wantType {
				t.Errorf("DimMeta[0].BaseType = %q, want %q", rec.DimMeta[0].BaseType, tt.wantType)
			}
		})
	}
}

func TestExtractAggs_Types(t *testing.T) {
	tests := []struct { //nolint:govet
		name      string
		entry     *pb.IngestAggEntry
		wantType  string
		wantValFn func(t *testing.T, val any)
	}{
		{
			name: "int value",
			entry: &pb.IngestAggEntry{
				Field:   "bytes",
				AggType: pb.IngestAggType_SUM,
				Value:   &pb.IngestAggEntry_IntVal{IntVal: 42},
			},
			wantType: "INT",
			wantValFn: func(t *testing.T, val any) {
				t.Helper()
				if val != int64(42) {
					t.Errorf("agg value = %v (%T), want int64(42)", val, val)
				}
			},
		},
		{
			name: "float value",
			entry: &pb.IngestAggEntry{
				Field:   "latency",
				AggType: pb.IngestAggType_AVG,
				Value:   &pb.IngestAggEntry_FloatVal{FloatVal: 1.5},
			},
			wantType: "FLOAT",
			wantValFn: func(t *testing.T, val any) {
				t.Helper()
				if val != 1.5 {
					t.Errorf("agg value = %v (%T), want 1.5", val, val)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &metastore.FileRecord{
				Dims: make(map[string]any),
				Aggs: make(map[string]any),
			}
			extractAggs([]*pb.IngestAggEntry{tt.entry}, rec)

			if len(rec.Aggs) != 1 {
				t.Fatalf("Aggs len = %d, want 1", len(rec.Aggs))
			}
			if len(rec.AggMeta) != 1 {
				t.Fatalf("AggMeta len = %d, want 1", len(rec.AggMeta))
			}
			if rec.AggMeta[0].ValueType != tt.wantType {
				t.Errorf("AggMeta[0].ValueType = %q, want %q", rec.AggMeta[0].ValueType, tt.wantType)
			}
			for _, v := range rec.Aggs {
				tt.wantValFn(t, v)
			}
		})
	}
}
