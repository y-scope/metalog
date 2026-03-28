package ingestion

import (
	"fmt"
	"sync"

	"github.com/y-scope/metalog/metastore"
)

// RecordTransformer transforms raw key-value data into structured FileRecord fields.
type RecordTransformer interface {
	// Transform processes a record, populating dynamic dimensions and aggregations.
	Transform(rec *metastore.FileRecord, data map[string]string) error
}

// DefaultTransformer is the default RecordTransformer that maps self-describing
// key-value entries to dimensions.
type DefaultTransformer struct{}

// NewDefaultTransformer creates a DefaultTransformer.
func NewDefaultTransformer() *DefaultTransformer {
	return &DefaultTransformer{}
}

// Transform populates the record's Dims from self-describing key-value data.
func (t *DefaultTransformer) Transform(rec *metastore.FileRecord, data map[string]string) error {
	if rec.Dims == nil {
		rec.Dims = make(map[string]any)
	}
	if rec.Aggs == nil {
		rec.Aggs = make(map[string]any)
	}
	for k, v := range data {
		rec.Dims[k] = v
	}
	return nil
}

var (
	transformerMu       sync.RWMutex
	transformerRegistry = map[string]func() RecordTransformer{
		"":        func() RecordTransformer { return NewDefaultTransformer() },
		"default": func() RecordTransformer { return NewDefaultTransformer() },
	}
)

// RegisterRecordTransformer registers a named record transformer factory.
func RegisterRecordTransformer(name string, factory func() RecordTransformer) {
	transformerMu.Lock()
	defer transformerMu.Unlock()
	transformerRegistry[name] = factory
}

// NewRecordTransformer creates a record transformer by name.
// Returns an error for unregistered names.
func NewRecordTransformer(name string) (RecordTransformer, error) {
	transformerMu.RLock()
	factory, ok := transformerRegistry[name]
	transformerMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown record transformer %q", name)
	}
	return factory(), nil
}
