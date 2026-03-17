package ingestion

import (
	"github.com/y-scope/metalog/internal/metastore"
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

// TransformerRegistry maps transformer names to implementations.
var TransformerRegistry = map[string]func() RecordTransformer{
	"":        func() RecordTransformer { return NewDefaultTransformer() },
	"default": func() RecordTransformer { return NewDefaultTransformer() },
}
