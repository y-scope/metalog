package node

import (
	"context"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/node/workerunit"
)

// WorkerUnit is an alias for workerunit.Unit so existing code in
// the node package (and callers) continues to compile without changes.
type WorkerUnit = workerunit.Unit

// NewWorkerUnit creates a worker unit derived from the given parent context.
func NewWorkerUnit(parent context.Context, concurrency int, nodeID string, shared *SharedResources, log *zap.Logger) *WorkerUnit {
	return workerunit.New(parent, concurrency, nodeID, shared, log)
}
