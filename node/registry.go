package node

import (
	"database/sql"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/node/registry"
)

// CoordinatorRegistry is an alias for registry.Registry so existing code in
// the node package (and callers) continues to compile without changes.
type CoordinatorRegistry = registry.Registry

// NewCoordinatorRegistry creates a CoordinatorRegistry.
func NewCoordinatorRegistry(db *sql.DB, nodeID string, isMariaDB bool, log *zap.Logger) *CoordinatorRegistry {
	return registry.New(db, nodeID, isMariaDB, log)
}
