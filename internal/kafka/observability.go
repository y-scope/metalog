package kafka

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
)

// DimSpec describes a dimension to extract from a normalized JSON payload.
// Width > 0 indicates a string dimension with max_length; Width == 0 indicates an int dimension.
type DimSpec struct {
	JSONKey string
	Width   int32
}

// ObservabilityTransformer transforms raw JSON payloads from observability platforms
// into MetadataRecord protos with platform-specific dimensions.
type ObservabilityTransformer struct {
	storageBackend string
	retentionDays  int32
	dims           []DimSpec
}

// NewObservabilityTransformer creates an ObservabilityTransformer.
func NewObservabilityTransformer(storageBackend string, retentionDays int32, dims []DimSpec) *ObservabilityTransformer {
	return &ObservabilityTransformer{
		storageBackend: storageBackend,
		retentionDays:  retentionDays,
		dims:           dims,
	}
}

func (t *ObservabilityTransformer) Transform(payload []byte) (*pb.MetadataRecord, error) {
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, fmt.Errorf("observability transform: %w", err)
	}

	// Normalize keys to lowercase
	normalized := make(map[string]any, len(raw))
	for k, v := range raw {
		normalized[strings.ToLower(k)] = v
	}

	// Extract IR path: strip leading "/"
	irPath, _ := getString(normalized, "tpath")
	irPath = strings.TrimPrefix(irPath, "/")

	// Extract timestamps: millis -> nanos
	creationTimeMs := getInt64(normalized, "creationtime")
	minTimestamp := creationTimeMs * 1_000_000

	lastModMs := getInt64(normalized, "lastmodification")
	if lastModMs == 0 {
		lastModMs = creationTimeMs
	}
	maxTimestamp := lastModMs * 1_000_000

	// Extract size
	sizeBytes := getInt64(normalized, "byteswritten")

	// Compute expires_at
	var expiresAt int64
	if t.retentionDays > 0 && minTimestamp > 0 {
		expiresAt = minTimestamp + int64(t.retentionDays)*24*int64(time.Hour)
	}

	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State:         "IR_ARCHIVE_BUFFERING",
			MinTimestamp:  minTimestamp,
			MaxTimestamp:  maxTimestamp,
			RetentionDays: t.retentionDays,
			ExpiresAt:     expiresAt,
			Ir: &pb.IrFileInfo{
				ClpIrStorageBackend: t.storageBackend,
				ClpIrPath:           irPath,
				ClpIrSizeBytes:      sizeBytes,
			},
		},
	}

	// Build dimension entries from specs
	for _, spec := range t.dims {
		val, ok := getString(normalized, spec.JSONKey)
		if !ok {
			continue
		}

		entry := &pb.DimEntry{Key: spec.JSONKey}
		if spec.Width > 0 {
			entry.Value = &pb.DimensionValue{
				Value: &pb.DimensionValue_Str{
					Str: &pb.StringDimension{Value: val, MaxLength: spec.Width},
				},
			}
		} else {
			intVal, _ := strconv.ParseInt(val, 10, 64)
			entry.Value = &pb.DimensionValue{
				Value: &pb.DimensionValue_IntVal{IntVal: intVal},
			}
		}
		record.Dim = append(record.Dim, entry)
	}

	return record, nil
}

func getString(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	switch val := v.(type) {
	case string:
		return val, true
	case float64:
		if val == float64(int64(val)) {
			return strconv.FormatInt(int64(val), 10), true
		}
		return strconv.FormatFloat(val, 'f', -1, 64), true
	default:
		return fmt.Sprintf("%v", val), true
	}
}

func getInt64(m map[string]any, key string) int64 {
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return int64(val)
	case string:
		n, _ := strconv.ParseInt(val, 10, 64)
		return n
	default:
		return 0
	}
}

// Platform-specific dimension specs.

var upDims = []DimSpec{
	{"service_name", 64}, {"instance", 16}, {"region", 16},
	{"zone", 16}, {"hostname", 16}, {"runtime_env", 32},
}

var rayDims = []DimSpec{
	{"project_name", 64}, {"job_name", 64}, {"user", 32},
	{"ray_cluster", 64}, {"ray_identifier", 64}, {"node_type", 16},
	{"pod_name", 64}, {"pod_uid", 64}, {"instance_id", 16},
	{"hostname", 64}, {"resource_pool", 32}, {"file", 128}, {"suffix", 16},
}

var warmStorageDims = []DimSpec{
	{"service_name", 64}, {"uber_region", 16}, {"user_zone", 16},
	{"runtime_env", 32},
}

var athenaDims = []DimSpec{
	{"app_name", 128}, {"app_name_hash", 16}, {"service_name", 64},
	{"yarn_app_id", 128}, {"container_log_id", 64}, {"odin_cluster", 64},
	{"deploy_env", 16}, {"hostname", 64},
}

var sparkDims = []DimSpec{
	{"spark_app_id", 128}, {"spark_app_name", 128}, {"spark_user", 64},
	{"queue_name", 128}, {"app_tier", 16}, {"hostname", 64},
	{"spark_container_type", 16},
	{"spark_app_attempt_id", 0}, // int
	{"spark_executor_id", 0},    // int
	{"spark_pod_uid", 64}, {"spark_yarn_container_id", 16},
	{"piper_pipeline", 128}, {"piper_task", 128}, {"piper_execution_date", 32},
	{"workflow_id", 128}, {"workflow_source", 128}, {"workflow_taskid", 128},
	{"workflow_tier", 16}, {"region", 16}, {"is_cloud", 8},
}

func init() {
	RegisterTransformer("up", func() MessageTransformer {
		return NewObservabilityTransformer("tb", 90, upDims)
	})
	RegisterTransformer("ray", func() MessageTransformer {
		return NewObservabilityTransformer("tb", 90, rayDims)
	})
	RegisterTransformer("warm-storage", func() MessageTransformer {
		return NewObservabilityTransformer("tb", 90, warmStorageDims)
	})
	RegisterTransformer("athena", func() MessageTransformer {
		return NewObservabilityTransformer("tb", 90, athenaDims)
	})
	RegisterTransformer("spark", func() MessageTransformer {
		return NewObservabilityTransformer("tb", 90, sparkDims)
	})
}
