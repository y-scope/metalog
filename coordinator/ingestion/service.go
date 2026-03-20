package ingestion

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/metastore"
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

// Ingest submits a single pre-converted record.
func (s *Service) Ingest(ctx context.Context, tableName string, rec *metastore.FileRecord) *IngestionResult {
	err := s.IngestWithCallback(ctx, tableName, rec, nil)
	if err != nil {
		return &IngestionResult{Accepted: false, Error: err.Error(), Err: err}
	}
	return &IngestionResult{Accepted: true}
}

// IngestWithCallback submits a single record (non-blocking).
// Returns ErrChannelFull immediately if the channel is full. If flushed is
// non-nil, the channel receives nil on successful DB write or a non-nil error
// on failure. The flushed channel must be buffered (cap >= 1).
func (s *Service) IngestWithCallback(ctx context.Context, tableName string, rec *metastore.FileRecord, flushed chan error) error {
	return s.ingestWithCallback(ctx, tableName, rec, flushed, false)
}

// IngestWithCallbackWait is like IngestWithCallback but blocks until the
// channel has space. Used by the Kafka consumer to avoid dropping messages.
func (s *Service) IngestWithCallbackWait(ctx context.Context, tableName string, rec *metastore.FileRecord, flushed chan error) error {
	return s.ingestWithCallback(ctx, tableName, rec, flushed, true)
}

func (s *Service) ingestWithCallback(ctx context.Context, tableName string, rec *metastore.FileRecord, flushed chan error, blocking bool) error {
	if tableName == "" {
		return &ValidationError{Msg: "table_name is required"}
	}
	if rec == nil {
		return fmt.Errorf("record is required")
	}

	rec.Flushed = flushed

	if blocking {
		return s.writer.SubmitWait(ctx, tableName, rec)
	}
	return s.writer.Submit(ctx, tableName, rec)
}
