package ingestion

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/schema"
)

// Service validates and submits metadata records for ingestion.
type Service struct {
	writer     *BatchingWriter
	log        *zap.Logger
	registries map[string]*schema.ColumnRegistry
	regMu      sync.RWMutex
}

// NewService creates an IngestionService.
func NewService(writer *BatchingWriter, log *zap.Logger) *Service {
	return &Service{
		writer:     writer,
		log:        log,
		registries: make(map[string]*schema.ColumnRegistry),
	}
}

// SetRegistry associates a column registry with a table for dim/agg resolution.
func (s *Service) SetRegistry(tableName string, reg *schema.ColumnRegistry) {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	s.registries[tableName] = reg
}

func (s *Service) getRegistry(tableName string) *schema.ColumnRegistry {
	s.regMu.RLock()
	defer s.regMu.RUnlock()
	return s.registries[tableName]
}

// IngestionResult provides details about the outcome of an ingestion request.
type IngestionResult struct {
	Accepted bool
	Error    string
}

// Ingest validates and submits a single record.
func (s *Service) Ingest(ctx context.Context, tableName string, record *pb.MetadataRecord) *IngestionResult {
	err := s.IngestWithCallback(ctx, tableName, record, nil)
	if err != nil {
		return &IngestionResult{Accepted: false, Error: err.Error()}
	}
	return &IngestionResult{Accepted: true}
}

// IngestWithCallback validates and submits a single record. If flushed is
// non-nil, it receives nil on successful DB write or an error on failure.
// The channel must be buffered (cap >= 1).
func (s *Service) IngestWithCallback(ctx context.Context, tableName string, record *pb.MetadataRecord, flushed chan error) error {
	if tableName == "" {
		return fmt.Errorf("table_name is required")
	}
	if err := validateRecord(record); err != nil {
		return err
	}

	rec := ConvertProtoToFileRecord(record)
	if rec == nil {
		return fmt.Errorf("failed to convert record")
	}
	rec.Flushed = flushed

	// Resolve dims and aggs to physical columns via registry.
	reg := s.getRegistry(tableName)
	if reg != nil {
		if err := s.resolveDims(ctx, record.Dim, rec, reg); err != nil {
			return fmt.Errorf("resolve dims: %w", err)
		}
		if err := s.resolveAggs(ctx, record.Agg, rec, reg); err != nil {
			return fmt.Errorf("resolve aggs: %w", err)
		}
	}

	return s.writer.Submit(ctx, tableName, rec)
}

func (s *Service) resolveDims(ctx context.Context, dims []*pb.DimEntry, rec *metastore.FileRecord, reg *schema.ColumnRegistry) error {
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

		col, err := reg.ResolveOrAllocateDim(ctx, d.Key, baseType, width)
		if err != nil {
			return fmt.Errorf("dim %q: %w", d.Key, err)
		}
		rec.Dims[col] = val
	}
	return nil
}

// validateRecord checks that a MetadataRecord has all required fields and valid values.
func validateRecord(record *pb.MetadataRecord) error {
	if record == nil || record.File == nil {
		return fmt.Errorf("record with file fields is required")
	}
	f := record.File
	if f.State == "" {
		return fmt.Errorf("state is required")
	}
	if f.MinTimestamp == 0 {
		return fmt.Errorf("min_timestamp is required")
	}
	if f.MaxTimestamp != 0 && f.MaxTimestamp < f.MinTimestamp {
		return fmt.Errorf("max_timestamp (%d) must be >= min_timestamp (%d)", f.MaxTimestamp, f.MinTimestamp)
	}
	// Validate state is a recognized value
	switch metastore.FileState(f.State) {
	case metastore.StateIRBuffering, metastore.StateIRClosed,
		metastore.StateIRArchiveBuffering, metastore.StateIRArchiveConsolidationPending,
		metastore.StateArchiveClosed, metastore.StateArchivePurging,
		metastore.StateIRPurging:
		// valid
	default:
		return fmt.Errorf("invalid state: %q", f.State)
	}
	// IR path is required for IR states
	state := metastore.FileState(f.State)
	needsIR := state == metastore.StateIRBuffering || state == metastore.StateIRClosed ||
		state == metastore.StateIRArchiveBuffering || state == metastore.StateIRArchiveConsolidationPending
	if needsIR && (f.Ir == nil || f.Ir.ClpIrPath == "") {
		return fmt.Errorf("clp_ir_path is required for state %s", f.State)
	}
	return nil
}

func (s *Service) resolveAggs(ctx context.Context, aggs []*pb.AggEntry, rec *metastore.FileRecord, reg *schema.ColumnRegistry) error {
	for _, a := range aggs {
		if a.Field == "" {
			continue
		}

		aggType := a.AggType.String()
		var valueType string
		var val any

		switch v := a.Value.(type) {
		case *pb.AggEntry_IntVal:
			valueType = "INT"
			val = v.IntVal
		case *pb.AggEntry_FloatVal:
			valueType = "FLOAT"
			val = v.FloatVal
		default:
			valueType = "INT"
			val = int64(0)
		}

		col, err := reg.ResolveOrAllocateAgg(ctx, a.Field, a.Qualifier, aggType, valueType)
		if err != nil {
			return fmt.Errorf("agg %q.%q: %w", a.Field, a.Qualifier, err)
		}
		rec.Aggs[col] = val
	}
	return nil
}
