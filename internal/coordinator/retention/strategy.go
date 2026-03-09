package retention

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/storage"
)

// Strategy defines the interface for a retention cleanup implementation.
// Each strategy runs as a per-table goroutine within CoordinatorUnit.
type Strategy interface {
	// Run executes the retention cleanup loop until ctx is canceled.
	Run(ctx context.Context)
}

// Deps holds shared dependencies injected into every strategy.
type Deps struct {
	DB              *sql.DB
	TableName       string
	IsMariaDB       bool
	StorageRegistry *storage.Registry
	Log             *zap.Logger
}

// StrategyMeta describes a registered strategy type.
type StrategyMeta struct {
	Factory func(deps Deps) (Strategy, error)
}

var (
	typeMu       sync.RWMutex
	typeRegistry = map[string]StrategyMeta{}
)

// RegisterType registers a strategy type factory.
// Called from init() in each strategy implementation.
func RegisterType(typeName string, meta StrategyMeta) {
	typeMu.Lock()
	defer typeMu.Unlock()
	typeRegistry[typeName] = meta
}

// CreateStrategy creates a strategy instance by type name.
func CreateStrategy(typeName string, deps Deps) (Strategy, error) {
	typeMu.RLock()
	meta, ok := typeRegistry[typeName]
	typeMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown retention strategy type: %q", typeName)
	}
	return meta.Factory(deps)
}
