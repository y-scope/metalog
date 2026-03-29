package query

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/schema"
)

// ResolvedAgg holds a single aggregation entry with its logical key, value,
// aggregation type, and typed result extracted from the database row.
type ResolvedAgg struct {
	Key             string
	Value           string
	AggregationType string // e.g. "SUM", "AVG", "MIN", "MAX", "EQ", ...
	ValueType       string // "FLOAT" or "INT"
	IntResult       int64
	FloatResult     float64
	HasResult       bool
}

// ResolvedSplit is a transport-agnostic representation of a split row with all
// physical column names (dim_fNN, agg_fNN) resolved back to logical names.
type ResolvedSplit struct {
	Dimensions map[string]string
	Aggs       []ResolvedAgg
	File       ResolvedFileInfo
}

// ResolvedFileInfo holds the __FILE.* fields from a split row.
type ResolvedFileInfo struct {
	ClpIRBucket              string
	ClpArchiveBucket         string
	State                    string
	ClpArchiveStorageBackend string
	ClpArchivePath           string
	ClpIRPath                string
	ClpIRStorageBackend      string
	ClpIRSizeBytes           int64
	MinTimestamp             int64
	RawSizeBytes             int64
	RecordCount              int64
	MaxTimestamp             int64
	ClpArchiveSizeBytes      int64
	ClpArchiveCreatedAt      int64
	ExpiresAt                int64
	RetentionDays            int32
}

// ResolveSplit converts a raw database SplitRow into a ResolvedSplit, mapping
// physical column names back to logical names using the ColumnRegistry.
func ResolveSplit(row *SplitRow, registry *schema.ColumnRegistry) *ResolvedSplit {
	rs := &ResolvedSplit{
		Dimensions: make(map[string]string),
	}
	f := &rs.File

	for col, val := range row.Values {
		switch col {
		case metastore.ColMinTimestamp:
			if v, ok := DBValToInt64(val); ok {
				f.MinTimestamp = v
			}
		case metastore.ColMaxTimestamp:
			if v, ok := DBValToInt64(val); ok {
				f.MaxTimestamp = v
			}
		case metastore.ColState:
			f.State = DBValToString(val)
		case metastore.ColRecordCount:
			if v, ok := DBValToInt64(val); ok {
				f.RecordCount = v
			}
		case metastore.ColRawSizeBytes:
			if v, ok := DBValToInt64(val); ok {
				f.RawSizeBytes = v
			}
		case metastore.ColClpIRPath:
			f.ClpIRPath = DBValToString(val)
		case metastore.ColClpIRStorageBackend:
			f.ClpIRStorageBackend = DBValToString(val)
		case metastore.ColClpIRBucket:
			f.ClpIRBucket = DBValToString(val)
		case metastore.ColClpIRSizeBytes:
			if v, ok := DBValToInt64(val); ok {
				f.ClpIRSizeBytes = v
			}
		case metastore.ColClpArchivePath:
			f.ClpArchivePath = DBValToString(val)
		case metastore.ColClpArchiveStorageBackend:
			f.ClpArchiveStorageBackend = DBValToString(val)
		case metastore.ColClpArchiveBucket:
			f.ClpArchiveBucket = DBValToString(val)
		case metastore.ColClpArchiveSizeBytes:
			if v, ok := DBValToInt64(val); ok {
				f.ClpArchiveSizeBytes = v
			}
		case metastore.ColClpArchiveCreatedAt:
			if v, ok := DBValToInt64(val); ok {
				f.ClpArchiveCreatedAt = v
			}
		case metastore.ColRetentionDays:
			if v, ok := DBValToInt64(val); ok {
				f.RetentionDays = int32(v)
			}
		case metastore.ColExpiresAt:
			if v, ok := DBValToInt64(val); ok {
				f.ExpiresAt = v
			}

		// Internal-only columns — not exposed, skip.
		case metastore.ColID, metastore.ColClpIRPathHash, metastore.ColClpArchivePathHash,
			metastore.ColSketches, metastore.ColExt:
			continue

		default:
			if val == nil {
				continue
			}
			// Aggregation columns: resolve physical name to logical entry.
			if strings.HasPrefix(col, metastore.AggColumnPrefix) && registry != nil {
				if entry := registry.LookupAggByColumn(col); entry != nil {
					ra := ResolvedAgg{
						Key:             entry.AggKey,
						Value:           entry.AggValue,
						AggregationType: entry.AggregationType,
						ValueType:       entry.ValueType,
					}
					if entry.ValueType == "FLOAT" {
						if f, ok := DBValToFloat64(val); ok {
							ra.FloatResult = f
							ra.HasResult = true
						}
					} else {
						if i, ok := DBValToInt64(val); ok {
							ra.IntResult = i
							ra.HasResult = true
						}
					}
					rs.Aggs = append(rs.Aggs, ra)
					continue
				}
			}
			// Dimension columns: resolve physical name to logical key.
			dimKey := col
			if strings.HasPrefix(col, metastore.DimColumnPrefix) && registry != nil {
				if entry := registry.LookupDimByColumn(col); entry != nil {
					dimKey = entry.DimKey
				}
			}
			rs.Dimensions[dimKey] = DBValToString(val)
		}
	}

	return rs
}

// DBValToInt64 converts a database interface{} value to int64.
func DBValToInt64(val any) (int64, bool) {
	switch v := val.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case uint32:
		return int64(v), true
	case float64:
		return int64(v), true
	case []byte:
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

// DBValToFloat64 converts a database interface{} value to float64.
func DBValToFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case []byte:
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// DBValToString converts a database interface{} value to a string.
// Handles []byte (from MySQL driver) and other types.
func DBValToString(val any) string {
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}
