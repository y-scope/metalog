package ingestion

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/y-scope/metalog/internal/metastore"
)

// jsonRecordTransformer parses a JSON payload field and flattens nested keys
// into the record's Dims and Aggs maps.
type jsonRecordTransformer struct {
	// PayloadKey is the data key containing the JSON string. Empty means the
	// entire data map is treated as the record fields.
	PayloadKey string
}

// newJSONRecordTransformer creates a jsonRecordTransformer.
func newJSONRecordTransformer(payloadKey string) *jsonRecordTransformer {
	return &jsonRecordTransformer{PayloadKey: payloadKey}
}

// Transform parses JSON from data[PayloadKey] (or all of data if PayloadKey is empty)
// and populates rec.Dims with flattened key-value pairs.
func (t *jsonRecordTransformer) Transform(rec *metastore.FileRecord, data map[string]string) error {
	if rec.Dims == nil {
		rec.Dims = make(map[string]any)
	}
	if rec.Aggs == nil {
		rec.Aggs = make(map[string]any)
	}

	if t.PayloadKey == "" {
		// Treat all data entries as top-level fields
		for k, v := range data {
			rec.Dims[k] = v
		}
		return nil
	}

	jsonStr, ok := data[t.PayloadKey]
	if !ok {
		return nil // no payload to parse
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		return fmt.Errorf("json transform: %w", err)
	}

	flattenJSON("", parsed, rec.Dims)
	return nil
}

// flattenJSON recursively flattens a nested JSON map into dot-separated keys.
func flattenJSON(prefix string, m map[string]any, out map[string]any) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}

		switch val := v.(type) {
		case map[string]any:
			flattenJSON(key, val, out)
		case []any:
			// Store arrays as JSON string
			if b, err := json.Marshal(val); err == nil {
				out[key] = string(b)
			}
		case float64:
			// Preserve numeric precision
			if val == float64(int64(val)) {
				out[key] = strconv.FormatInt(int64(val), 10)
			} else {
				out[key] = strconv.FormatFloat(val, 'f', -1, 64)
			}
		case bool:
			out[key] = strconv.FormatBool(val)
		case nil:
			// skip nil values
		default:
			out[key] = fmt.Sprintf("%v", val)
		}
	}
}

func init() {
	TransformerRegistry["json"] = func() RecordTransformer {
		return newJSONRecordTransformer("")
	}
}
