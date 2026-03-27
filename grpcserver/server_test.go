package grpcserver

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/y-scope/metalog/coordinator/ingestion"
	coordpb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
	ingpb "github.com/y-scope/metalog/gen/proto/ingestionpb"
)

// --- Server lifecycle tests ---

func TestNewServer(t *testing.T) {
	s := NewServer(0, zap.NewNop())
	if s == nil {
		t.Fatal("expected non-nil server")
	}
	if s.GRPCServer() == nil {
		t.Fatal("expected non-nil gRPC server")
	}
	s.Stop()
}

func TestServer_StopWithoutStart(t *testing.T) {
	s := NewServer(0, zap.NewNop())
	// Stop without Start should not panic
	s.Stop()
}

// --- IngestionHandler tests ---

func TestIngestionHandler_Ingest_NilRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := ingestion.NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		ingestion.WithBatchSize(100),
		ingestion.WithFlushInterval(10*time.Second),
	)
	defer bw.Stop()

	svc := ingestion.NewService(bw, false, zap.NewNop())
	h := NewIngestionHandler(svc, zap.NewNop())

	req := &ingpb.IngestRequest{
		TableName: "test_table",
		Record:    nil,
	}
	_, err := h.Ingest(ctx, req)
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestIngestionHandler_Ingest_MissingFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := ingestion.NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		ingestion.WithBatchSize(100),
		ingestion.WithFlushInterval(10*time.Second),
	)
	defer bw.Stop()

	svc := ingestion.NewService(bw, false, zap.NewNop())
	h := NewIngestionHandler(svc, zap.NewNop())

	req := &ingpb.IngestRequest{
		TableName: "test_table",
		Record:    &ingpb.MetadataRecord{},
	}
	_, err := h.Ingest(ctx, req)
	assertGRPCCode(t, err, codes.InvalidArgument)
}

// --- RegisterKafkaSource validation tests ---

func TestRegisterKafkaSource_Validation(t *testing.T) {
	tests := []struct {
		name        string
		req         *coordpb.RegisterKafkaSourceRequest
		wantMsgFrag string
	}{
		{
			name:        "EmptyTableName",
			req:         &coordpb.RegisterKafkaSourceRequest{},
			wantMsgFrag: "table_name is required",
		},
		{
			name: "EmptySourceName",
			req: &coordpb.RegisterKafkaSourceRequest{
				TableName: "test_table",
			},
			wantMsgFrag: "source_name is required",
		},
		{
			name: "EmptyTopic",
			req: &coordpb.RegisterKafkaSourceRequest{
				TableName:  "test_table",
				SourceName: "src1",
			},
			wantMsgFrag: "topic is required",
		},
		{
			name: "EmptyBootstrapServers",
			req: &coordpb.RegisterKafkaSourceRequest{
				TableName:  "test_table",
				SourceName: "src1",
				Topic:      "test-topic",
			},
			wantMsgFrag: "bootstrap_servers is required",
		},
		{
			name: "EmptyConsumerGroup",
			req: &coordpb.RegisterKafkaSourceRequest{
				TableName:        "test_table",
				SourceName:       "src1",
				Topic:            "test-topic",
				BootstrapServers: "broker:9092",
			},
			wantMsgFrag: "consumer_group_id is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestAdminHandler(t)
			_, err := h.RegisterKafkaSource(context.Background(), tc.req)
			assertGRPCCode(t, err, codes.InvalidArgument)
			assertContains(t, status.Convert(err).Message(), tc.wantMsgFrag)
		})
	}
}

// --- DeleteKafkaSource validation tests ---

func TestDeleteKafkaSource_Validation(t *testing.T) {
	tests := []struct {
		req  *coordpb.DeleteKafkaSourceRequest
		name string
	}{
		{
			name: "EmptyTableName",
			req:  &coordpb.DeleteKafkaSourceRequest{},
		},
		{
			name: "EmptySourceName",
			req:  &coordpb.DeleteKafkaSourceRequest{TableName: "test_table"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestAdminHandler(t)
			_, err := h.DeleteKafkaSource(context.Background(), tc.req)
			assertGRPCCode(t, err, codes.InvalidArgument)
		})
	}
}

// --- ListTables error ---

func TestMapAdminError_UnknownError(t *testing.T) {
	err := mapAdminError(context.DeadlineExceeded)
	st := status.Convert(err)
	if st.Code() != codes.Internal {
		t.Errorf("code = %v, want Internal", st.Code())
	}
}

func TestIngestionHandler_Ingest_Success(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	db, _, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	bw := ingestion.NewBatchingWriter(ctx, db, false, zap.NewNop(),
		ingestion.WithBatchSize(10000),
		ingestion.WithFlushInterval(time.Hour),
	)

	svc := ingestion.NewService(bw, false, zap.NewNop())
	h := NewIngestionHandler(svc, zap.NewNop())

	req := &ingpb.IngestRequest{
		TableName: "test_table",
		Record: &ingpb.MetadataRecord{
			File: &ingpb.FileFields{
				State:        "IR_BUFFERING",
				MinTimestamp: 1000,
				Ir:           &ingpb.IrFileInfo{ClpIrPath: "/test.ir"},
			},
		},
	}
	resp, err := h.Ingest(ctx, req)
	if err != nil {
		t.Fatalf("Ingest error: %v", err)
	}
	if !resp.Accepted {
		t.Error("expected Accepted=true")
	}

	cancel()
	bw.Stop()
}
