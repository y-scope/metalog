package ingestion

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/schema"
)

// Service validates and submits metadata records for ingestion.
// Dim/agg values are extracted using logical keys and stored on the FileRecord
// with type metadata. Physical column resolution and schema evolution happen
// at batch flush time in the BatchingWriter.
type Service struct {
	writer *BatchingWriter
	log    *zap.Logger
}

// NewService creates a Service.
func NewService(writer *BatchingWriter, log *zap.Logger) *Service {
	return &Service{
		writer: writer,
		log:    log,
	}
}

// ValidationError wraps a validation failure so the gRPC handler can
// distinguish it from transport/backpressure errors.
type ValidationError struct {
	Msg string
}

func (e *ValidationError) Error() string { return e.Msg }

// IngestionResult provides details about the outcome of an ingestion request.
type IngestionResult struct {
	Accepted bool
	Error    string
	Err      error // raw error for gRPC status mapping
}

// Ingest validates and submits a single record.
func (s *Service) Ingest(ctx context.Context, tableName string, record *pb.MetadataRecord) *IngestionResult {
	err := s.IngestWithCallback(ctx, tableName, record, nil)
	if err != nil {
		return &IngestionResult{Accepted: false, Error: err.Error(), Err: err}
	}
	return &IngestionResult{Accepted: true}
}

// IngestWithCallback validates and submits a single record (non-blocking).
// Returns ErrChannelFull immediately if the channel is full. If flushed is
// non-nil, the channel receives nil on successful DB write or a non-nil error
// on failure. The flushed channel must be buffered (cap >= 1).
func (s *Service) IngestWithCallback(ctx context.Context, tableName string, record *pb.MetadataRecord, flushed chan error) error {
	return s.ingestWithCallback(ctx, tableName, record, flushed, false)
}

// IngestWithCallbackWait is like IngestWithCallback but blocks until the
// channel has space. Used by the Kafka consumer to avoid dropping messages.
func (s *Service) IngestWithCallbackWait(ctx context.Context, tableName string, record *pb.MetadataRecord, flushed chan error) error {
	return s.ingestWithCallback(ctx, tableName, record, flushed, true)
}

func (s *Service) ingestWithCallback(ctx context.Context, tableName string, record *pb.MetadataRecord, flushed chan error, blocking bool) error {
	if tableName == "" {
		return &ValidationError{Msg: "table_name is required"}
	}
	if err := validateRecord(record); err != nil {
		return err
	}

	rec := FileRecordFromProto(record)
	if rec == nil {
		return fmt.Errorf("failed to convert record")
	}
	rec.Flushed = flushed

	// Extract dim/agg/sketch values and type metadata from proto. Physical column
	// resolution happens at batch flush time (BatchingWriter.flushBatch).
	extractDims(record.Dim, rec)
	extractAggs(record.Agg, rec)
	extractSketches(record.Sketch, rec)

	if blocking {
		return s.writer.SubmitWait(ctx, tableName, rec)
	}
	return s.writer.Submit(ctx, tableName, rec)
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
