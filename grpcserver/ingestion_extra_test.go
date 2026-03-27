package grpcserver

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	ingpb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/coordinator/ingestion"
)

func TestIngestionHandler_Ingest_MissingAllFields(t *testing.T) {
	// ConvertRecord should fail validation when File has no state/timestamps
	ctx := context.Background()
	// We don't need a real BW since ConvertRecord fails before Service.Ingest
	h := &IngestionHandler{log: zap.NewNop()}

	req := &ingpb.IngestRequest{
		TableName: "test_table",
		Record: &ingpb.MetadataRecord{
			File: &ingpb.FileFields{},
		},
	}

	_, err := h.Ingest(ctx, req)
	if err == nil {
		t.Error("expected error for empty record fields")
	}
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestNewIngestionHandler_NotNil(t *testing.T) {
	ctx := context.Background()
	bw := ingestion.NewBatchingWriter(ctx, nil, false, zap.NewNop())
	defer bw.Stop()
	svc := ingestion.NewService(bw, false, zap.NewNop())
	h := NewIngestionHandler(svc, zap.NewNop())
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
}
