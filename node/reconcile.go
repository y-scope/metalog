package node

import (
	"context"
	"time"
)

// reconcileRegistry defines the database operations used by the reconciliation loop.
// Extracted as an interface so reconcile() can be tested with a mock registry.
type reconcileRegistry interface {
	ClaimOrphansLease(ctx context.Context, leaseTTL time.Duration) ([]string, error)
	ClaimOrphansHeartbeat(ctx context.Context, deadThreshold time.Duration) ([]string, error)
	GetUnassignedTables(ctx context.Context) ([]string, error)
	CountActiveNodesLease(ctx context.Context) (int, error)
	CountActiveNodesHeartbeat(ctx context.Context, deadThreshold time.Duration) (int, error)
	CountMyTables(ctx context.Context) (int, error)
	CountAssignedTables(ctx context.Context) (int, error)
	ClaimTable(ctx context.Context, tableName string, leaseTTL time.Duration) (bool, error)
	GetAssignedTables(ctx context.Context) ([]string, error)
	ReleaseTable(ctx context.Context, tableName string) error
}

// coordinatorLifecycle defines the operations on a running coordinator unit
// needed by the reconciliation loop. CoordinatorUnit satisfies this interface.
type coordinatorLifecycle interface {
	IsStalled() bool
	Restart() error
	Stop()
}
