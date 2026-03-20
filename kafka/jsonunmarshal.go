package kafka

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/vmihailenco/msgpack/v5"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
)

// unmarshalJSONToProto parses a JSON payload into a MetadataRecord.
// Supports a flat JSON format with fields matching the proto message.
func unmarshalJSONToProto(payload []byte) (*pb.MetadataRecord, error) {
	var raw struct {
		State        string `json:"state"`
		MinTimestamp int64  `json:"min_timestamp"`
		MaxTimestamp int64  `json:"max_timestamp"`
		RawSizeBytes int64  `json:"raw_size_bytes"`
		RecordCount  int32  `json:"record_count"`
		IR           *struct {
			StorageBackend string `json:"storage_backend"`
			Bucket         string `json:"bucket"`
			Path           string `json:"path"`
			SizeBytes      int64  `json:"size_bytes"`
		} `json:"ir"`
		Dims []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
			Width int32  `json:"width"`
		} `json:"dims"`
		Aggs []struct {
			Field     string `json:"field"`
			Qualifier string `json:"qualifier"`
			Type      string `json:"type"`
			IntVal    int64  `json:"int_val"`
		} `json:"aggs"`
		SelfDescribingKV []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"self_describing_kv"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}

	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State:        raw.State,
			MinTimestamp: raw.MinTimestamp,
			MaxTimestamp: raw.MaxTimestamp,
			RawSizeBytes: raw.RawSizeBytes,
			RecordCount:  raw.RecordCount,
		},
	}

	if raw.IR != nil {
		record.File.Ir = &pb.IrFileInfo{
			ClpIrStorageBackend: raw.IR.StorageBackend,
			ClpIrBucket:         raw.IR.Bucket,
			ClpIrPath:           raw.IR.Path,
			ClpIrSizeBytes:      raw.IR.SizeBytes,
		}
	}

	for _, d := range raw.Dims {
		width := d.Width
		if width == 0 {
			width = 64
		}
		record.Dim = append(record.Dim, &pb.DimEntry{
			Key: d.Key,
			Value: &pb.DimensionValue{
				Value: &pb.DimensionValue_Str{
					Str: &pb.StringDimension{Value: d.Value, MaxLength: width},
				},
			},
		})
	}

	for _, a := range raw.Aggs {
		aggType := pb.IngestAggType_GTE
		if a.Type != "" {
			if v, ok := pb.IngestAggType_value[a.Type]; ok {
				aggType = pb.IngestAggType(v)
			} else {
				return nil, fmt.Errorf("unrecognized agg type %q for field %q", a.Type, a.Field)
			}
		}
		record.Agg = append(record.Agg, &pb.IngestAggEntry{
			Field:     a.Field,
			Qualifier: a.Qualifier,
			AggType:   aggType,
			Value:     &pb.IngestAggEntry_IntVal{IntVal: a.IntVal},
		})
	}

	// Parse self-describing key-value entries. The key prefix determines the
	// entry type:
	//   sketch/{type}/{field}              → SketchEntry (value is base64 raw filter bytes)
	//   dim/{typeSpec}/{field}             → DimEntry
	//   agg_int/{type}/{field}[/{qual}]   → AggEntry (INT)
	//   agg_float/{type}/{field}[/{qual}] → AggEntry (FLOAT)
	for _, kv := range raw.SelfDescribingKV {
		if err := parseSelfDescribingEntry(kv.Key, kv.Value, record); err != nil {
			// Skip malformed entries rather than dropping the entire record.
			// The entry is passed through as-is so it can be inspected later.
			record.SelfDescribingKv = append(record.SelfDescribingKv, &pb.SelfDescribingEntry{
				Key:   kv.Key,
				Value: kv.Value,
			})
		}
	}

	return record, nil
}

// parseSelfDescribingEntry parses a slash-delimited self-describing key and
// appends the appropriate typed entry to the MetadataRecord.
func parseSelfDescribingEntry(key, value string, record *pb.MetadataRecord) error {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) < 2 || parts[1] == "" {
		// Pass through as-is to SelfDescribingKv for unknown prefixes.
		record.SelfDescribingKv = append(record.SelfDescribingKv, &pb.SelfDescribingEntry{
			Key:   key,
			Value: value,
		})
		return nil
	}

	switch {
	case parts[0] == "sketch":
		// sketch/{type}/{field} — value is base64-encoded raw filter bytes.
		// The type is in the key, so we reconstruct the msgpack snapshot.
		return parseSelfDescribingSketch(parts[1], value, record)

	case parts[0] == "dim":
		// dim/{typeSpec}/{field} — parse type spec and field name
		return parseSelfDescribingDim(parts[1], value, record)

	case parts[0] == "agg_int":
		// agg_int/{type}/{field}[/{qualifier}]
		return parseSelfDescribingAgg(parts[1], value, "INT", record)

	case parts[0] == "agg_float":
		// agg_float/{type}/{field}[/{qualifier}]
		return parseSelfDescribingAgg(parts[1], value, "FLOAT", record)

	default:
		record.SelfDescribingKv = append(record.SelfDescribingKv, &pb.SelfDescribingEntry{
			Key:   key,
			Value: value,
		})
	}
	return nil
}

// parseSelfDescribingSketch parses the remainder after "sketch/" and appends a SketchEntry.
// Format: {type}/{field} — value is base64-encoded raw filter bytes.
// The type and data are re-encoded as msgpack to produce the BloomFilterSnapshot
// format that the ingestion pipeline expects.
func parseSelfDescribingSketch(remainder, value string, record *pb.MetadataRecord) error {
	parts := strings.SplitN(remainder, "/", 2)
	if len(parts) < 2 || parts[1] == "" {
		return fmt.Errorf("missing field name in sketch key")
	}
	sketchType, field := parts[0], parts[1]

	rawData, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return fmt.Errorf("base64 decode: %w", err)
	}

	// Re-encode as msgpack BloomFilterSnapshot: {type: "<type>", data: <bytes>}
	snapshot := struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	}{
		Type: sketchType,
		Data: rawData,
	}
	snapshotBytes, err := msgpack.Marshal(&snapshot)
	if err != nil {
		return fmt.Errorf("encode sketch snapshot: %w", err)
	}

	record.Sketch = append(record.Sketch, &pb.SketchEntry{
		SketchKey: field,
		Data:      snapshotBytes,
	})
	return nil
}

// parseSelfDescribingDim parses the remainder after "dim/" and appends a DimEntry.
// Format: {typeSpec}/{field}
//
//	str{N}/{field}       → str, width N
//	str{N}utf8/{field}   → str_utf8, width N
//	int/{field}          → int
//	float/{field}        → float
//	bool/{field}         → bool
func parseSelfDescribingDim(remainder, value string, record *pb.MetadataRecord) error {
	parts := strings.SplitN(remainder, "/", 2)
	if len(parts) < 2 || parts[1] == "" {
		return fmt.Errorf("missing field name in dim key")
	}
	typeSpec, field := parts[0], parts[1]

	var dimVal *pb.DimensionValue
	switch {
	case typeSpec == "int":
		var intVal int64
		if _, err := fmt.Sscanf(value, "%d", &intVal); err != nil {
			return fmt.Errorf("parse int dim value: %w", err)
		}
		dimVal = &pb.DimensionValue{Value: &pb.DimensionValue_IntVal{IntVal: intVal}}
	case typeSpec == "float":
		var floatVal float64
		if _, err := fmt.Sscanf(value, "%g", &floatVal); err != nil {
			return fmt.Errorf("parse float dim value: %w", err)
		}
		dimVal = &pb.DimensionValue{Value: &pb.DimensionValue_FloatVal{FloatVal: floatVal}}
	case typeSpec == "bool":
		boolVal := value == "true" || value == "1"
		dimVal = &pb.DimensionValue{Value: &pb.DimensionValue_BoolVal{BoolVal: boolVal}}
	case strings.HasSuffix(typeSpec, "utf8"):
		width := parseWidthFromTypeSpec(strings.TrimSuffix(typeSpec, "utf8"))
		dimVal = &pb.DimensionValue{Value: &pb.DimensionValue_StrUtf8{
			StrUtf8: &pb.StringDimension{Value: value, MaxLength: int32(width)},
		}}
	default:
		// str{N} — default string type
		width := parseWidthFromTypeSpec(typeSpec)
		dimVal = &pb.DimensionValue{Value: &pb.DimensionValue_Str{
			Str: &pb.StringDimension{Value: value, MaxLength: int32(width)},
		}}
	}

	record.Dim = append(record.Dim, &pb.DimEntry{Key: field, Value: dimVal})
	return nil
}

// parseWidthFromTypeSpec extracts the numeric width from a type spec like "str128".
// Strips the "str" prefix if present. Defaults to 64 if parsing fails.
func parseWidthFromTypeSpec(spec string) int {
	spec = strings.TrimPrefix(spec, "str")
	var width int
	if _, err := fmt.Sscanf(spec, "%d", &width); err != nil || width <= 0 {
		return 64
	}
	return width
}

// parseSelfDescribingAgg parses the remainder after "agg_int/" or "agg_float/"
// and appends an IngestAggEntry.
// Format: {type}/{field}[/{qualifier}]
func parseSelfDescribingAgg(remainder, value, valueType string, record *pb.MetadataRecord) error {
	parts := strings.SplitN(remainder, "/", 3)
	if len(parts) < 2 || parts[1] == "" {
		return fmt.Errorf("missing field name in agg key")
	}
	aggTypeStr := strings.ToUpper(parts[0])
	field := parts[1]
	var qualifier string
	if len(parts) == 3 {
		qualifier = parts[2]
	}

	aggType := pb.IngestAggType_GTE
	if v, ok := pb.IngestAggType_value[aggTypeStr]; ok {
		aggType = pb.IngestAggType(v)
	} else {
		return fmt.Errorf("unrecognized agg type %q for field %q", aggTypeStr, field)
	}

	entry := &pb.IngestAggEntry{
		Field:     field,
		Qualifier: qualifier,
		AggType:   aggType,
	}

	if valueType == "FLOAT" {
		var floatVal float64
		if _, err := fmt.Sscanf(value, "%g", &floatVal); err != nil {
			return fmt.Errorf("parse float agg value: %w", err)
		}
		entry.Value = &pb.IngestAggEntry_FloatVal{FloatVal: floatVal}
	} else {
		var intVal int64
		if _, err := fmt.Sscanf(value, "%d", &intVal); err != nil {
			return fmt.Errorf("parse int agg value: %w", err)
		}
		entry.Value = &pb.IngestAggEntry_IntVal{IntVal: intVal}
	}

	record.Agg = append(record.Agg, entry)
	return nil
}
