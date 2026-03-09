package ingestion

import (
	"database/sql"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/internal/metastore"
)

// FileRecordFromProto converts a protobuf MetadataRecord to a FileRecord.
func FileRecordFromProto(record *pb.MetadataRecord) *metastore.FileRecord {
	if record == nil || record.File == nil {
		return nil
	}

	f := record.File
	rec := &metastore.FileRecord{
		State:         metastore.FileState(f.State),
		MinTimestamp:  f.MinTimestamp,
		MaxTimestamp:  f.MaxTimestamp,
		RawSizeBytes:  sql.NullInt64{Int64: f.RawSizeBytes, Valid: f.RawSizeBytes > 0},
		RecordCount:   uint32(f.RecordCount),
		RetentionDays: uint16(f.RetentionDays),
		ExpiresAt:     f.ExpiresAt,
		Dims:          make(map[string]any),
		Aggs:          make(map[string]any),
	}

	if f.Ir != nil {
		rec.ClpIRStorageBackend = toNullString(f.Ir.ClpIrStorageBackend)
		rec.ClpIRBucket = toNullString(f.Ir.ClpIrBucket)
		rec.ClpIRPath = toNullString(f.Ir.ClpIrPath)
		if f.Ir.ClpIrSizeBytes > 0 {
			rec.ClpIRSizeBytes = sql.NullInt32{Int32: int32(f.Ir.ClpIrSizeBytes), Valid: true}
		}
	}

	if f.Archive != nil {
		rec.ClpArchiveStorageBackend = toNullString(f.Archive.ClpArchiveStorageBackend)
		rec.ClpArchiveBucket = toNullString(f.Archive.ClpArchiveBucket)
		rec.ClpArchivePath = toNullString(f.Archive.ClpArchivePath)
		rec.ClpArchiveCreatedAt = f.Archive.ClpArchiveCreatedAt
		if f.Archive.ClpArchiveSizeBytes > 0 {
			rec.ClpArchiveSizeBytes = sql.NullInt32{Int32: int32(f.Archive.ClpArchiveSizeBytes), Valid: true}
		}
	}

	return rec
}

func toNullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
