package node

import (
	"database/sql"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/schema"
	"github.com/y-scope/metalog/storage"
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


// Close releases all shared resources.
func (s *Resources) Close() {
	if s.ReadDB != nil {
		if err := s.ReadDB.Close(); err != nil {
			s.Log.Warn("failed to close read DB", zap.Error(err))
		}
	}
	if s.DB != nil {
		if err := s.DB.Close(); err != nil {
			s.Log.Warn("failed to close DB", zap.Error(err))
		}
	}
}
