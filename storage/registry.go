package storage

import (
	"fmt"
	"sync"
)

// BackendMeta describes a registered backend type.
type BackendMeta struct {
	RequiresBucket bool
	Factory        func(cfg map[string]string) (Backend, error)
}

var (
	typeMu       sync.RWMutex
	typeRegistry = map[string]BackendMeta{}
)

// RegisterType registers a backend type factory.
func RegisterType(typeName string, meta BackendMeta) {
	typeMu.Lock()
	defer typeMu.Unlock()
	typeRegistry[typeName] = meta
}

// CreateBackend creates a backend instance by type name.
func CreateBackend(typeName string, cfg map[string]string) (Backend, error) {
	typeMu.RLock()
	meta, ok := typeRegistry[typeName]
	typeMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown storage backend type: %q", typeName)
	}
	return meta.Factory(cfg)
}

// RequiresBucket returns whether a backend type uses buckets.
func RequiresBucket(typeName string) bool {
	typeMu.RLock()
	defer typeMu.RUnlock()
	if meta, ok := typeRegistry[typeName]; ok {
		return meta.RequiresBucket
	}
	return true
}

// Registry maps backend names to Backend instances.
type Registry struct {
	mu       sync.RWMutex
	backends map[string]Backend
}

// NewRegistry creates a storage registry.
func NewRegistry() *Registry {
	return &Registry{backends: make(map[string]Backend)}
}

// Register adds a named backend.
func (r *Registry) Register(name string, backend Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backends[name] = backend
}

// Get returns a backend by name.
func (r *Registry) Get(name string) (Backend, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.backends[name]
	if !ok {
		return nil, fmt.Errorf("storage backend %q not registered", name)
	}
	return b, nil
}
