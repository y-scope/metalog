package ingestion

import (
	"testing"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/internal/metastore"
)

func TestConvertProtoToFileRecord_NilInput(t *testing.T) {
	result := ConvertProtoToFileRecord(nil)
	if result != nil {
		t.Error("ConvertProtoToFileRecord(nil) should return nil")
	}
}

func TestConvertProtoToFileRecord_NilFile(t *testing.T) {
	record := &pb.MetadataRecord{File: nil}
	result := ConvertProtoToFileRecord(record)
	if result != nil {
		t.Error("ConvertProtoToFileRecord with nil File should return nil")
	}
}

func TestConvertProtoToFileRecord_BasicFields(t *testing.T) {
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

	result := ConvertProtoToFileRecord(record)

	if result == nil {
		t.Fatal("ConvertProtoToFileRecord returned nil")
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

func TestConvertProtoToFileRecord_RawSizeBytesZero(t *testing.T) {
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State:        "IR_BUFFERING",
			RawSizeBytes: 0,
		},
	}

	result := ConvertProtoToFileRecord(record)

	// Zero or negative should result in Invalid NullInt64
	if result.RawSizeBytes.Valid {
		t.Error("RawSizeBytes should be invalid when 0")
	}
}

func TestConvertProtoToFileRecord_IRFields(t *testing.T) {
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

	result := ConvertProtoToFileRecord(record)

	if !result.ClpIRStorageBackend.Valid || result.ClpIRStorageBackend.String != "s3" {
		t.Errorf("ClpIRStorageBackend = %v, want s3", result.ClpIRStorageBackend)
	}
	if !result.ClpIRBucket.Valid || result.ClpIRBucket.String != "logs-bucket" {
		t.Errorf("ClpIRBucket = %v, want logs-bucket", result.ClpIRBucket)
	}
	if !result.ClpIRPath.Valid || result.ClpIRPath.String != "/path/to/file.ir" {
		t.Errorf("ClpIRPath = %v, want /path/to/file.ir", result.ClpIRPath)
	}
	if !result.ClpIRSizeBytes.Valid || result.ClpIRSizeBytes.Int32 != 2048 {
		t.Errorf("ClpIRSizeBytes = %v, want 2048", result.ClpIRSizeBytes)
	}
}

func TestConvertProtoToFileRecord_ArchiveFields(t *testing.T) {
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

	result := ConvertProtoToFileRecord(record)

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
	if !result.ClpArchiveSizeBytes.Valid || result.ClpArchiveSizeBytes.Int32 != 4096 {
		t.Errorf("ClpArchiveSizeBytes = %v, want 4096", result.ClpArchiveSizeBytes)
	}
}

func TestConvertProtoToFileRecord_NoArchive(t *testing.T) {
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State:   "IR_BUFFERING",
			Archive: nil,
		},
	}

	result := ConvertProtoToFileRecord(record)

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

func TestConvertProtoToFileRecord_DimsAndAggsInitialized(t *testing.T) {
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State: "IR_BUFFERING",
		},
	}

	result := ConvertProtoToFileRecord(record)

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

func TestConvertProtoToFileRecord_IRSizeBytesZero(t *testing.T) {
	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State: "IR_BUFFERING",
			Ir: &pb.IrFileInfo{
				ClpIrStorageBackend: "s3",
				ClpIrSizeBytes:      0,
			},
		},
	}

	result := ConvertProtoToFileRecord(record)

	// Zero size should be invalid
	if result.ClpIRSizeBytes.Valid {
		t.Error("ClpIRSizeBytes should be invalid when 0")
	}
}
