package storage

import (
	"errors"
	"fmt"
)

// Sentinel errors for storage operations.
var (
	ErrAccessDenied = errors.New("storage: access denied")
	ErrRateLimited  = errors.New("storage: rate limited")
	ErrTimeout      = errors.New("storage: timeout")
)

// OpError wraps an underlying error with the operation context that caused it.
type OpError struct {
	Op     string // operation: "get", "put", "delete", "exists"
	Bucket string
	Key    string
	Err    error
}

func (e *OpError) Error() string {
	return fmt.Sprintf("storage %s %s/%s: %v", e.Op, e.Bucket, e.Key, e.Err)
}

func (e *OpError) Unwrap() error {
	return e.Err
}

// IsNotFound returns true if the error indicates the object does not exist.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrObjectNotFound)
}

// IsAccessDenied returns true if the error is an access denial.
func IsAccessDenied(err error) bool {
	return errors.Is(err, ErrAccessDenied)
}

// IsRateLimited returns true if the error is a rate limit.
func IsRateLimited(err error) bool {
	return errors.Is(err, ErrRateLimited)
}

// IsTimeout returns true if the error is a timeout.
func IsTimeout(err error) bool {
	return errors.Is(err, ErrTimeout)
}

// IsRetryable returns true if the error is transient and the operation can be retried.
func IsRetryable(err error) bool {
	return IsRateLimited(err) || IsTimeout(err)
}
