package storage

import (
	"fmt"
	"sync"
)

// Registry maps backend names to StorageBackend instances.
type Registry struct {
	mu       sync.RWMutex
	backends map[string]StorageBackend
}

// NewRegistry creates a storage registry.
func NewRegistry() *Registry {
	return &Registry{backends: make(map[string]StorageBackend)}
}

// Register adds a named backend.
func (r *Registry) Register(name string, backend StorageBackend) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backends[name] = backend
}

// Get returns a backend by name.
func (r *Registry) Get(name string) (StorageBackend, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.backends[name]
	if !ok {
		return nil, fmt.Errorf("storage backend %q not registered", name)
	}
	return b, nil
}
