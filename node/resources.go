package node

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/schema"
	"github.com/y-scope/metalog/telemetry"
)

// Resources holds resources shared across all units in a node.
// The Node owns these resources and closes them after all units have stopped.
type Resources struct {
	DB                 *sql.DB
	ReadDB             *sql.DB
	Log                *zap.Logger
	Telemetry          *telemetry.Provider
	registries         map[string]*schema.ColumnRegistry
	FailureLogInterval time.Duration
	regMu              sync.RWMutex
	IsMariaDB          bool
	dbOwned            bool
	readDBOwned        bool
}

// ReadOnlyDB returns the read-only pool if configured, otherwise the RW pool.
func (s *Resources) ReadOnlyDB() *sql.DB {
	if s.ReadDB != nil {
		return s.ReadDB
	}
	return s.DB
}

// SetColumnRegistry adds or replaces a column registry for a table.
func (s *Resources) SetColumnRegistry(tableName string, reg *schema.ColumnRegistry) {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	if s.registries == nil {
		s.registries = make(map[string]*schema.ColumnRegistry)
	}
	s.registries[tableName] = reg
}

// GetColumnRegistry returns the column registry for a table.
// If no registry is cached (e.g. the table is not assigned to this node),
// one is loaded on-demand from the database. This allows any node to serve
// read-only queries regardless of table assignment.
func (s *Resources) GetColumnRegistry(tableName string) *schema.ColumnRegistry {
	// Fast path: check cache.
	s.regMu.RLock()
	if reg, ok := s.registries[tableName]; ok {
		s.regMu.RUnlock()
		return reg
	}
	s.regMu.RUnlock()

	// Slow path: load from DB (read-only operation).
	reg, err := schema.NewColumnRegistry(context.Background(), s.ReadOnlyDB(), tableName, s.IsMariaDB, s.Log)
	if err != nil {
		s.Log.Warn("failed to load column registry on demand",
			zap.String("table", tableName), zap.Error(err))
		return nil
	}

	// Cache for future queries. If a coordinator starts for this table later,
	// SetColumnRegistry will overwrite with its own registry.
	s.SetColumnRegistry(tableName, reg)
	return reg
}

// Close releases all shared resources. Database pools are only closed
// when they were created by the Node (not injected via WithDB/WithReadDB).
func (s *Resources) Close() {
	if s.Telemetry != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Telemetry.Shutdown(shutdownCtx); err != nil {
			s.Log.Warn("failed to shutdown telemetry", zap.Error(err))
		}
	}
	if s.ReadDB != nil && s.readDBOwned {
		if err := s.ReadDB.Close(); err != nil {
			s.Log.Warn("failed to close read DB", zap.Error(err))
		}
	}
	if s.DB != nil && s.dbOwned {
		if err := s.DB.Close(); err != nil {
			s.Log.Warn("failed to close DB", zap.Error(err))
		}
	}
}
