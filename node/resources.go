package node

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/schema"
	"github.com/y-scope/metalog/storage"
	"github.com/y-scope/metalog/telemetry"
)

// Resources holds resources shared across all units in a node.
// The Node owns these resources and closes them after all units have stopped.
type Resources struct {
	DB              *sql.DB // RW pool (primary). Nil in RO-only deployments.
	ReadDB          *sql.DB // RO pool (read replica). Nil when not configured.
	StorageRegistry *storage.Registry
	ArchiveCreator  *storage.ArchiveCreator
	ArchiveBackend  string
	ArchiveBucket   string
	IsMariaDB       bool
	Log             *zap.Logger

	// FailureLogInterval controls how often periodic loops repeat failure
	// warnings. Set from logging.failureLogIntervalSeconds in node.yaml.
	FailureLogInterval time.Duration

	// Telemetry provides the OpenTelemetry MeterProvider for metrics.
	// Nil when telemetry is disabled (subsystems use no-op meters).
	Telemetry *telemetry.Provider

	// dbOwned/readDBOwned track whether Node created the pools (and should
	// close them on Stop). When pools are injected via WithDB/WithReadDB,
	// the caller retains ownership and these are false.
	dbOwned     bool
	readDBOwned bool

	regMu      sync.RWMutex
	registries map[string]*schema.ColumnRegistry
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

// GetColumnRegistry returns the column registry for a table, or nil.
func (s *Resources) GetColumnRegistry(tableName string) *schema.ColumnRegistry {
	s.regMu.RLock()
	defer s.regMu.RUnlock()
	return s.registries[tableName]
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
