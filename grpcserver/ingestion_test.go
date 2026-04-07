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

func TestMapIngestionError(t *testing.T) {
	tests := []struct {
		err  error
		name string
	}{
		{
			name: "ValidationError",
			err:  &ingestion.ValidationError{Msg: "state is required"},
		},
		{
			name: "GenericError",
			err:  fmt.Errorf("something broke"),
		},
		{
			name: "WrappedValidationError",
			err:  fmt.Errorf("convert: %w", &ingestion.ValidationError{Msg: "bad field"}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			grpcErr := mapIngestionError(tc.err)

			st, ok := status.FromError(grpcErr)
			if !ok {
				t.Fatal("expected gRPC status error")
			}
			if st.Code() != codes.InvalidArgument {
				t.Errorf("code = %v, want InvalidArgument", st.Code())
			}
		})
	}
}

// TestMapIngestionError_ValidationError_MessagePropagated verifies that a
// direct ValidationError has its message forwarded to the gRPC status.
func TestMapIngestionError_ValidationError_MessagePropagated(t *testing.T) {
	err := &ingestion.ValidationError{Msg: "state is required"}
	grpcErr := mapIngestionError(err)

	st, ok := status.FromError(grpcErr)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Message() != "state is required" {
		t.Errorf("message = %q, want %q", st.Message(), "state is required")
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

func TestMapToGRPCError(t *testing.T) {
	tests := []struct {
		err      error
		name     string
		wantCode codes.Code
		wantNil  bool
	}{
		{
			name:     "ChannelFull",
			err:      ingestion.ErrChannelFull,
			wantCode: codes.ResourceExhausted,
		},
		{
			name:     "WrappedChannelFull",
			err:      fmt.Errorf("submit: %w", ingestion.ErrChannelFull),
			wantCode: codes.ResourceExhausted,
		},
		{
			name:     "DeadlineExceeded",
			err:      context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:     "WrappedDeadlineExceeded",
			err:      fmt.Errorf("submit: %w", context.DeadlineExceeded),
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:     "Canceled",
			err:      context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "ValidationError",
			err:      &ingestion.ValidationError{Msg: "bad"},
			wantCode: codes.InvalidArgument,
		},
		{
			name:    "UnknownError_ReturnsNil",
			err:     fmt.Errorf("internal failure"),
			wantNil: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := &IngestionHandler{}
			result := &ingestion.IngestionResult{Err: tc.err}
			grpcErr := h.mapToGRPCError(result)

			if tc.wantNil {
				if grpcErr != nil {
					t.Errorf("expected nil for unknown error, got %v", grpcErr)
				}
				return
			}

			st, _ := status.FromError(grpcErr)
			if st.Code() != tc.wantCode {
				t.Errorf("code = %v, want %v", st.Code(), tc.wantCode)
			}
		})
	}
}
