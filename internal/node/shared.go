package node

import (
	"database/sql"
	"sync"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/schema"
	"github.com/y-scope/metalog/storage"
	"github.com/y-scope/metalog/internal/taskqueue"
)

// SharedResources holds resources shared across all units in a node.
// The Node owns these resources and closes them after all units have stopped.
type SharedResources struct {
	DB              *sql.DB
	WorkerDB        *sql.DB // optional separate pool for workers; falls back to DB
	StorageRegistry *storage.Registry
	ArchiveCreator  *storage.ArchiveCreator
	ArchiveBackend  string
	ArchiveBucket   string
	TaskQueue       *taskqueue.Queue
	IsMariaDB       bool
	Log             *zap.Logger

	regMu      sync.RWMutex
	registries map[string]*schema.ColumnRegistry
}

// SetColumnRegistry adds or replaces a column registry for a table.
func (s *SharedResources) SetColumnRegistry(tableName string, reg *schema.ColumnRegistry) {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	if s.registries == nil {
		s.registries = make(map[string]*schema.ColumnRegistry)
	}
	s.registries[tableName] = reg
}

// GetColumnRegistry returns the column registry for a table, or nil.
func (s *SharedResources) GetColumnRegistry(tableName string) *schema.ColumnRegistry {
	s.regMu.RLock()
	defer s.regMu.RUnlock()
	return s.registries[tableName]
}

// ColumnRegistries returns a snapshot of all registries (safe for concurrent use).
func (s *SharedResources) ColumnRegistries() map[string]*schema.ColumnRegistry {
	s.regMu.RLock()
	defer s.regMu.RUnlock()
	m := make(map[string]*schema.ColumnRegistry, len(s.registries))
	for k, v := range s.registries {
		m[k] = v
	}
	return m
}

// EffectiveWorkerDB returns the worker DB pool, or falls back to the main DB.
func (s *SharedResources) EffectiveWorkerDB() *sql.DB {
	if s.WorkerDB != nil {
		return s.WorkerDB
	}
	return s.DB
}

// Close releases all shared resources.
func (s *SharedResources) Close() {
	if s.WorkerDB != nil && s.WorkerDB != s.DB {
		if err := s.WorkerDB.Close(); err != nil {
			s.Log.Warn("failed to close worker DB", zap.Error(err))
		}
	}
	if s.DB != nil {
		if err := s.DB.Close(); err != nil {
			s.Log.Warn("failed to close DB", zap.Error(err))
		}
	}
}
