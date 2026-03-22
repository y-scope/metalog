package ingestion

import (
	"database/sql"
	"fmt"
	"math"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/schema"
)

// ConvertRecord validates a proto MetadataRecord and converts it to a
// fully-populated FileRecord with dims, aggs, and sketches extracted.
func ConvertRecord(record *pb.MetadataRecord) (*metastore.FileRecord, error) {
	if err := validateRecord(record); err != nil {
		return nil, err
	}

	rec, err := fileRecordFromProto(record)
	if err != nil {
		return nil, err
	}
	extractDims(record.Dim, rec)
	extractAggs(record.Agg, rec)
	extractSketches(record.Sketch, rec)

	return rec, nil
}

// validateRecord checks that a MetadataRecord has all required fields and valid values.
func validateRecord(record *pb.MetadataRecord) error {
	if record == nil {
		return &ValidationError{Msg: "record is required"}
	}
	if record.File == nil {
		return &ValidationError{Msg: "record.File is required"}
	}
	f := record.File
	if f.State == "" {
		return &ValidationError{Msg: "state is required"}
	}
	if f.MinTimestamp == 0 {
		return &ValidationError{Msg: "min_timestamp is required"}
	}
	if f.MaxTimestamp != 0 && f.MaxTimestamp < f.MinTimestamp {
		return &ValidationError{Msg: fmt.Sprintf("max_timestamp (%d) must be >= min_timestamp (%d)", f.MaxTimestamp, f.MinTimestamp)}
	}
	// Validate state is a recognized value
	switch metastore.FileState(f.State) {
	case metastore.StateIRBuffering, metastore.StateIRClosed,
		metastore.StateIRArchiveBuffering, metastore.StateIRArchiveConsolidationPending,
		metastore.StateArchiveClosed, metastore.StateArchivePurging,
		metastore.StateIRPurging:
		// valid
	default:
		return &ValidationError{Msg: fmt.Sprintf("invalid state: %q", f.State)}
	}
	// IR path is required for IR states
	state := metastore.FileState(f.State)
	needsIR := state == metastore.StateIRBuffering || state == metastore.StateIRClosed ||
		state == metastore.StateIRArchiveBuffering || state == metastore.StateIRArchiveConsolidationPending
	if needsIR && (f.Ir == nil || f.Ir.ClpIrPath == "") {
		return &ValidationError{Msg: fmt.Sprintf("clp_ir_path is required for state %s", f.State)}
	}
	return nil
}

// fileRecordFromProto converts a protobuf MetadataRecord to a FileRecord.
func fileRecordFromProto(record *pb.MetadataRecord) (*metastore.FileRecord, error) {
	if record == nil || record.File == nil {
		return nil, nil
	}

	f := record.File

	expiresAt := f.ExpiresAt
	retentionDays := f.RetentionDays
	if retentionDays == 0 {
		retentionDays = metastore.DefaultRetentionDays
	}
	if expiresAt == 0 && f.MinTimestamp > 0 {
		expiresAt = f.MinTimestamp + int64(retentionDays)*86400*1e9
	}

	rec := &metastore.FileRecord{
		State:         metastore.FileState(f.State),
		MinTimestamp:  f.MinTimestamp,
		MaxTimestamp:  f.MaxTimestamp,
		RawSizeBytes:  sql.NullInt64{Int64: f.RawSizeBytes, Valid: f.RawSizeBytes > 0},
		RecordCount:   uint32(f.RecordCount),
		RetentionDays: uint16(retentionDays),
		ExpiresAt:     expiresAt,
		Dims:          make(map[string]any),
		Aggs:          make(map[string]any),
		Sketches:      make(map[string][]byte),
	}

	if f.Ir != nil {
		rec.ClpIRStorageBackend = toNullString(f.Ir.ClpIrStorageBackend)
		rec.ClpIRBucket = toNullString(f.Ir.ClpIrBucket)
		rec.ClpIRPath = toNullString(f.Ir.ClpIrPath)
		if f.Ir.ClpIrSizeBytes > 0 {
			if f.Ir.ClpIrSizeBytes > math.MaxUint32 {
				return nil, &ValidationError{Msg: fmt.Sprintf("clp_ir_size_bytes %d exceeds INT UNSIGNED max", f.Ir.ClpIrSizeBytes)}
			}
			rec.ClpIRSizeBytes = sql.NullInt64{Int64: f.Ir.ClpIrSizeBytes, Valid: true}
		}
	}

	if f.Archive != nil {
		rec.ClpArchiveStorageBackend = toNullString(f.Archive.ClpArchiveStorageBackend)
		rec.ClpArchiveBucket = toNullString(f.Archive.ClpArchiveBucket)
		rec.ClpArchivePath = toNullString(f.Archive.ClpArchivePath)
		rec.ClpArchiveCreatedAt = f.Archive.ClpArchiveCreatedAt
		if f.Archive.ClpArchiveSizeBytes > 0 {
			if f.Archive.ClpArchiveSizeBytes > math.MaxUint32 {
				return nil, &ValidationError{Msg: fmt.Sprintf("clp_archive_size_bytes %d exceeds INT UNSIGNED max", f.Archive.ClpArchiveSizeBytes)}
			}
			rec.ClpArchiveSizeBytes = sql.NullInt64{Int64: f.Archive.ClpArchiveSizeBytes, Valid: true}
		}
	}

	return rec, nil
}

func toNullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// extractDims populates FileRecord.Dims with logical keys and FileRecord.DimMeta
// with type metadata for schema evolution.
func extractDims(dims []*pb.DimEntry, rec *metastore.FileRecord) {
	for _, d := range dims {
		if d.Key == "" || d.Value == nil {
			continue
		}

		var baseType string
		var width int
		var val any

		switch v := d.Value.Value.(type) {
		case *pb.DimensionValue_Str:
			baseType = "str"
			width = int(v.Str.MaxLength)
			val = v.Str.Value
		case *pb.DimensionValue_StrUtf8:
			baseType = "str_utf8"
			width = int(v.StrUtf8.MaxLength)
			val = v.StrUtf8.Value
		case *pb.DimensionValue_IntVal:
			baseType = "int"
			val = v.IntVal
		case *pb.DimensionValue_BoolVal:
			baseType = "bool"
			val = v.BoolVal
		case *pb.DimensionValue_FloatVal:
			baseType = "float"
			val = v.FloatVal
		default:
			continue
		}

		rec.Dims[d.Key] = val
		rec.DimMeta = append(rec.DimMeta, metastore.DimMeta{
			Key:      d.Key,
			BaseType: baseType,
			Width:    width,
		})
	}
}

// extractAggs populates FileRecord.Aggs with logical keys and FileRecord.AggMeta
// with type metadata for schema evolution.
func extractAggs(aggs []*pb.IngestAggEntry, rec *metastore.FileRecord) {
	for _, a := range aggs {
		if a.Field == "" {
			continue
		}

		aggType := a.AggType.String()
		var valueType string
		var val any

		switch v := a.Value.(type) {
		case *pb.IngestAggEntry_IntVal:
			valueType = "INT"
			val = v.IntVal
		case *pb.IngestAggEntry_FloatVal:
			valueType = "FLOAT"
			val = v.FloatVal
		default:
			continue
		}

		// Key by logical composite key so batch flush can resolve to physical.
		logicalKey := schema.AggCacheKey(a.Field, a.Qualifier, aggType)
		rec.Aggs[logicalKey] = val
		rec.AggMeta = append(rec.AggMeta, metastore.AggMeta{
			Key:       a.Field,
			Value:     a.Qualifier,
			Type:      aggType,
			ValueType: valueType,
			AliasCol:  a.AliasColumn,
		})
	}
}

// extractSketches populates FileRecord.Sketches with logical keys.
func extractSketches(sketches []*pb.SketchEntry, rec *metastore.FileRecord) {
	for _, s := range sketches {
		if s.SketchKey == "" || len(s.Data) == 0 {
			continue
		}
		rec.Sketches[s.SketchKey] = s.Data
	}
}
