package grpcserver

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/y-scope/metalog/coordinator/ingestion"
)

// --- mapIngestionError tests ---

func TestMapIngestionError_ValidationError(t *testing.T) {
	err := &ingestion.ValidationError{Msg: "state is required"}
	grpcErr := mapIngestionError(err)

	st, ok := status.FromError(grpcErr)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
	if st.Message() != "state is required" {
		t.Errorf("message = %q, want %q", st.Message(), "state is required")
	}
}

func TestMapIngestionError_GenericError(t *testing.T) {
	err := fmt.Errorf("something broke")
	grpcErr := mapIngestionError(err)

	st, _ := status.FromError(grpcErr)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestMapIngestionError_WrappedValidationError(t *testing.T) {
	inner := &ingestion.ValidationError{Msg: "bad field"}
	err := fmt.Errorf("convert: %w", inner)
	grpcErr := mapIngestionError(err)

	st, _ := status.FromError(grpcErr)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

// --- mapToGRPCError tests ---

func TestMapToGRPCError_NilError(t *testing.T) {
	h := &IngestionHandler{}
	result := &ingestion.IngestionResult{Err: nil}
	if grpcErr := h.mapToGRPCError(result); grpcErr != nil {
		t.Errorf("expected nil, got %v", grpcErr)
	}
}

func TestMapToGRPCError_ChannelFull(t *testing.T) {
	h := &IngestionHandler{}
	result := &ingestion.IngestionResult{Err: ingestion.ErrChannelFull}
	grpcErr := h.mapToGRPCError(result)

	st, _ := status.FromError(grpcErr)
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("code = %v, want ResourceExhausted", st.Code())
	}
}

func TestMapToGRPCError_WrappedChannelFull(t *testing.T) {
	h := &IngestionHandler{}
	result := &ingestion.IngestionResult{Err: fmt.Errorf("submit: %w", ingestion.ErrChannelFull)}
	grpcErr := h.mapToGRPCError(result)

	st, _ := status.FromError(grpcErr)
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("code = %v, want ResourceExhausted (wrapped)", st.Code())
	}
}

func TestMapToGRPCError_DeadlineExceeded(t *testing.T) {
	h := &IngestionHandler{}
	result := &ingestion.IngestionResult{Err: context.DeadlineExceeded}
	grpcErr := h.mapToGRPCError(result)

	st, _ := status.FromError(grpcErr)
	if st.Code() != codes.DeadlineExceeded {
		t.Errorf("code = %v, want DeadlineExceeded", st.Code())
	}
}

func TestMapToGRPCError_WrappedDeadlineExceeded(t *testing.T) {
	h := &IngestionHandler{}
	result := &ingestion.IngestionResult{Err: fmt.Errorf("submit: %w", context.DeadlineExceeded)}
	grpcErr := h.mapToGRPCError(result)

	st, _ := status.FromError(grpcErr)
	if st.Code() != codes.DeadlineExceeded {
		t.Errorf("code = %v, want DeadlineExceeded (wrapped)", st.Code())
	}
}

func TestMapToGRPCError_Canceled(t *testing.T) {
	h := &IngestionHandler{}
	result := &ingestion.IngestionResult{Err: context.Canceled}
	grpcErr := h.mapToGRPCError(result)

	st, _ := status.FromError(grpcErr)
	if st.Code() != codes.Canceled {
		t.Errorf("code = %v, want Canceled", st.Code())
	}
}

func TestMapToGRPCError_ValidationError(t *testing.T) {
	h := &IngestionHandler{}
	result := &ingestion.IngestionResult{Err: &ingestion.ValidationError{Msg: "bad"}}
	grpcErr := h.mapToGRPCError(result)

	st, _ := status.FromError(grpcErr)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestMapToGRPCError_UnknownError_ReturnsNil(t *testing.T) {
	h := &IngestionHandler{}
	result := &ingestion.IngestionResult{Err: fmt.Errorf("internal failure")}
	grpcErr := h.mapToGRPCError(result)

	if grpcErr != nil {
		t.Errorf("expected nil for unknown error, got %v", grpcErr)
	}
}
