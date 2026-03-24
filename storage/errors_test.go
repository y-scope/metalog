package storage

import (
	"errors"
	"testing"
)

func TestOpError_Error(t *testing.T) {
	err := &OpError{Op: "get", Bucket: "b", Key: "k", Err: ErrObjectNotFound}
	want := "storage get b/k: object not found"
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestOpError_Unwrap(t *testing.T) {
	err := &OpError{Op: "put", Bucket: "b", Key: "k", Err: ErrAccessDenied}
	if !errors.Is(err, ErrAccessDenied) {
		t.Error("errors.Is should match ErrAccessDenied")
	}
}

func TestIsNotFound(t *testing.T) {
	if !IsNotFound(ErrObjectNotFound) {
		t.Error("IsNotFound should return true for ErrObjectNotFound")
	}
	if IsNotFound(ErrAccessDenied) {
		t.Error("IsNotFound should return false for ErrAccessDenied")
	}
}

func TestIsRetryable(t *testing.T) {
	if !IsRetryable(ErrRateLimited) {
		t.Error("ErrRateLimited should be retryable")
	}
	if !IsRetryable(ErrTimeout) {
		t.Error("ErrTimeout should be retryable")
	}
	if IsRetryable(ErrAccessDenied) {
		t.Error("ErrAccessDenied should not be retryable")
	}
	if IsRetryable(ErrObjectNotFound) {
		t.Error("ErrObjectNotFound should not be retryable")
	}
}
