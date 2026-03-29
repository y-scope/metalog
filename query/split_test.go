package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/y-scope/metalog/metastore"
)

// --- DBValToString tests ---

func TestDBValToString_String(t *testing.T) {
	require.Equal(t, "hello", DBValToString("hello"))
}

func TestDBValToString_Bytes(t *testing.T) {
	require.Equal(t, "world", DBValToString([]byte("world")))
}

func TestDBValToString_Nil(t *testing.T) {
	require.Equal(t, "", DBValToString(nil))
}

func TestDBValToString_Int(t *testing.T) {
	// Falls back to fmt.Sprintf for unknown types.
	require.Equal(t, "42", DBValToString(42))
}

func TestDBValToString_Float(t *testing.T) {
	require.Equal(t, "3.14", DBValToString(3.14))
}

// --- DBValToInt64 tests ---

func TestDBValToInt64_Int64(t *testing.T) {
	v, ok := DBValToInt64(int64(100))
	require.True(t, ok)
	require.Equal(t, int64(100), v)
}

func TestDBValToInt64_Int32(t *testing.T) {
	v, ok := DBValToInt64(int32(32))
	require.True(t, ok)
	require.Equal(t, int64(32), v)
}

func TestDBValToInt64_Uint32(t *testing.T) {
	v, ok := DBValToInt64(uint32(64))
	require.True(t, ok)
	require.Equal(t, int64(64), v)
}

func TestDBValToInt64_Float64(t *testing.T) {
	v, ok := DBValToInt64(float64(9.0))
	require.True(t, ok)
	require.Equal(t, int64(9), v)
}

func TestDBValToInt64_BytesValid(t *testing.T) {
	v, ok := DBValToInt64([]byte("123"))
	require.True(t, ok)
	require.Equal(t, int64(123), v)
}

func TestDBValToInt64_BytesInvalid(t *testing.T) {
	_, ok := DBValToInt64([]byte("not-a-number"))
	require.False(t, ok)
}

func TestDBValToInt64_StringValid(t *testing.T) {
	v, ok := DBValToInt64("456")
	require.True(t, ok)
	require.Equal(t, int64(456), v)
}

func TestDBValToInt64_StringInvalid(t *testing.T) {
	_, ok := DBValToInt64("abc")
	require.False(t, ok)
}

func TestDBValToInt64_Nil(t *testing.T) {
	_, ok := DBValToInt64(nil)
	require.False(t, ok)
}

func TestDBValToInt64_Bool(t *testing.T) {
	_, ok := DBValToInt64(true)
	require.False(t, ok)
}

// --- DBValToFloat64 tests ---

func TestDBValToFloat64_Float64(t *testing.T) {
	v, ok := DBValToFloat64(float64(3.14))
	require.True(t, ok)
	require.InDelta(t, 3.14, v, 1e-9)
}

func TestDBValToFloat64_Int64(t *testing.T) {
	v, ok := DBValToFloat64(int64(7))
	require.True(t, ok)
	require.Equal(t, float64(7), v)
}

func TestDBValToFloat64_Int32(t *testing.T) {
	v, ok := DBValToFloat64(int32(5))
	require.True(t, ok)
	require.Equal(t, float64(5), v)
}

func TestDBValToFloat64_BytesValid(t *testing.T) {
	v, ok := DBValToFloat64([]byte("2.718"))
	require.True(t, ok)
	require.InDelta(t, 2.718, v, 1e-9)
}

func TestDBValToFloat64_BytesInvalid(t *testing.T) {
	_, ok := DBValToFloat64([]byte("not-a-float"))
	require.False(t, ok)
}

func TestDBValToFloat64_StringValid(t *testing.T) {
	v, ok := DBValToFloat64("1.5")
	require.True(t, ok)
	require.Equal(t, float64(1.5), v)
}

func TestDBValToFloat64_StringInvalid(t *testing.T) {
	_, ok := DBValToFloat64("xyz")
	require.False(t, ok)
}

func TestDBValToFloat64_Nil(t *testing.T) {
	_, ok := DBValToFloat64(nil)
	require.False(t, ok)
}

func TestDBValToFloat64_Bool(t *testing.T) {
	_, ok := DBValToFloat64(true)
	require.False(t, ok)
}

// --- ResolveSplit tests ---

func TestResolveSplit_FileColumns(t *testing.T) {
	row := &SplitRow{
		Values: map[string]any{
			metastore.ColMinTimestamp:             int64(1000),
			metastore.ColMaxTimestamp:             int64(2000),
			metastore.ColState:                    "IR_CLOSED",
			metastore.ColRecordCount:              int64(500),
			metastore.ColRawSizeBytes:             int64(8192),
			metastore.ColClpIRPath:                "/logs/ir/file.clp.zst",
			metastore.ColClpIRStorageBackend:      "s3",
			metastore.ColClpIRBucket:              "my-bucket",
			metastore.ColClpIRSizeBytes:           int64(4096),
			metastore.ColClpArchivePath:           "/logs/archive/file.clp",
			metastore.ColClpArchiveStorageBackend: "s3",
			metastore.ColClpArchiveBucket:         "archive-bucket",
			metastore.ColClpArchiveSizeBytes:      int64(2048),
			metastore.ColClpArchiveCreatedAt:      int64(3000),
			metastore.ColRetentionDays:            int64(30),
			metastore.ColExpiresAt:                int64(99999),
		},
	}

	rs := ResolveSplit(row, nil)

	require.Equal(t, int64(1000), rs.File.MinTimestamp)
	require.Equal(t, int64(2000), rs.File.MaxTimestamp)
	require.Equal(t, "IR_CLOSED", rs.File.State)
	require.Equal(t, int64(500), rs.File.RecordCount)
	require.Equal(t, int64(8192), rs.File.RawSizeBytes)
	require.Equal(t, "/logs/ir/file.clp.zst", rs.File.ClpIRPath)
	require.Equal(t, "s3", rs.File.ClpIRStorageBackend)
	require.Equal(t, "my-bucket", rs.File.ClpIRBucket)
	require.Equal(t, int64(4096), rs.File.ClpIRSizeBytes)
	require.Equal(t, "/logs/archive/file.clp", rs.File.ClpArchivePath)
	require.Equal(t, "s3", rs.File.ClpArchiveStorageBackend)
	require.Equal(t, "archive-bucket", rs.File.ClpArchiveBucket)
	require.Equal(t, int64(2048), rs.File.ClpArchiveSizeBytes)
	require.Equal(t, int64(3000), rs.File.ClpArchiveCreatedAt)
	require.Equal(t, int32(30), rs.File.RetentionDays)
	require.Equal(t, int64(99999), rs.File.ExpiresAt)
}

func TestResolveSplit_InternalColumnsSkipped(t *testing.T) {
	// Internal-only columns must not appear in dimensions.
	row := &SplitRow{
		Values: map[string]any{
			metastore.ColID:                 int64(1),
			metastore.ColClpIRPathHash:      []byte("hash1"),
			metastore.ColClpArchivePathHash: []byte("hash2"),
			metastore.ColSketches:           []byte("sketch-data"),
			metastore.ColExt:                []byte("ext-data"),
		},
	}

	rs := ResolveSplit(row, nil)

	require.Empty(t, rs.Dimensions)
	require.Empty(t, rs.Aggs)
}

func TestResolveSplit_DimColumnsWithRegistry(t *testing.T) {
	reg := newTestRegistry(t)

	row := &SplitRow{
		Values: map[string]any{
			"dim_f01": "us-east-1",
			"dim_f02": "my-api-service",
		},
	}

	rs := ResolveSplit(row, reg)

	require.Equal(t, "us-east-1", rs.Dimensions["region"])
	require.Equal(t, "my-api-service", rs.Dimensions["service.name"])
}

func TestResolveSplit_DimColumnsNilRegistry(t *testing.T) {
	// Without a registry the physical column name becomes the dimension key.
	row := &SplitRow{
		Values: map[string]any{
			"dim_f01": "us-west-2",
		},
	}

	rs := ResolveSplit(row, nil)

	// dim_f01 has the "dim_f" prefix but registry is nil, so it falls through
	// to the dimension default-key path; the physical name is used as the key.
	require.Equal(t, "us-west-2", rs.Dimensions["dim_f01"])
}

func TestResolveSplit_AggIntColumn(t *testing.T) {
	reg := newTestRegistry(t)

	row := &SplitRow{
		Values: map[string]any{
			"agg_f01": int64(200), // status_code, INT, EQ
		},
	}

	rs := ResolveSplit(row, reg)

	require.Len(t, rs.Aggs, 1)
	agg := rs.Aggs[0]
	require.Equal(t, "status_code", agg.Key)
	require.Equal(t, "EQ", agg.AggregationType)
	require.Equal(t, "INT", agg.ValueType)
	require.Equal(t, int64(200), agg.IntResult)
	require.True(t, agg.HasResult)
}

func TestResolveSplit_AggFloatColumn(t *testing.T) {
	reg := newTestRegistry(t)

	row := &SplitRow{
		Values: map[string]any{
			"agg_f02": float64(99.5), // response_time.p99, FLOAT, GTE
		},
	}

	rs := ResolveSplit(row, reg)

	require.Len(t, rs.Aggs, 1)
	agg := rs.Aggs[0]
	require.Equal(t, "response_time", agg.Key)
	require.Equal(t, "p99", agg.Value)
	require.Equal(t, "GTE", agg.AggregationType)
	require.Equal(t, "FLOAT", agg.ValueType)
	require.InDelta(t, 99.5, agg.FloatResult, 1e-9)
	require.True(t, agg.HasResult)
}

func TestResolveSplit_AggNilValueSkipped(t *testing.T) {
	reg := newTestRegistry(t)

	row := &SplitRow{
		Values: map[string]any{
			"agg_f01": nil, // nil agg values are skipped entirely
		},
	}

	rs := ResolveSplit(row, reg)

	require.Empty(t, rs.Aggs)
}

func TestResolveSplit_AggUnknownColumnFallsToDim(t *testing.T) {
	// An agg_f prefix column not in the registry falls through to the
	// dimensions map using the physical column name as key.
	reg := newTestRegistry(t)

	row := &SplitRow{
		Values: map[string]any{
			"agg_f99": "orphan", // not registered
		},
	}

	rs := ResolveSplit(row, reg)

	require.Empty(t, rs.Aggs)
	require.Equal(t, "orphan", rs.Dimensions["agg_f99"])
}

func TestResolveSplit_UnknownColumnStoredAsDim(t *testing.T) {
	// Unrecognised columns (no special prefix) land in the dimensions map.
	row := &SplitRow{
		Values: map[string]any{
			"custom_col": "custom-value",
		},
	}

	rs := ResolveSplit(row, nil)

	require.Equal(t, "custom-value", rs.Dimensions["custom_col"])
}

func TestResolveSplit_NilUnknownColumnSkipped(t *testing.T) {
	// nil values for unknown/unrecognised columns are skipped.
	row := &SplitRow{
		Values: map[string]any{
			"some_col": nil,
		},
	}

	rs := ResolveSplit(row, nil)

	require.Empty(t, rs.Dimensions)
}

func TestResolveSplit_EmptyRow(t *testing.T) {
	row := &SplitRow{Values: map[string]any{}}
	rs := ResolveSplit(row, nil)

	require.NotNil(t, rs)
	require.Empty(t, rs.Dimensions)
	require.Empty(t, rs.Aggs)
}

func TestResolveSplit_StateFromBytes(t *testing.T) {
	// MySQL drivers may return string columns as []byte.
	row := &SplitRow{
		Values: map[string]any{
			metastore.ColState: []byte("ARCHIVE_ONLY"),
		},
	}

	rs := ResolveSplit(row, nil)

	require.Equal(t, "ARCHIVE_ONLY", rs.File.State)
}

func TestResolveSplit_IntColumnFromBytes(t *testing.T) {
	// Numeric columns returned as []byte (e.g. MariaDB TEXT-mode protocol).
	row := &SplitRow{
		Values: map[string]any{
			metastore.ColRecordCount: []byte("750"),
		},
	}

	rs := ResolveSplit(row, nil)

	require.Equal(t, int64(750), rs.File.RecordCount)
}

func TestResolveSplit_AggIntColumnFromBytes(t *testing.T) {
	reg := newTestRegistry(t)

	row := &SplitRow{
		Values: map[string]any{
			"agg_f03": []byte("42"), // bytes_sent, INT, SUM
		},
	}

	rs := ResolveSplit(row, reg)

	require.Len(t, rs.Aggs, 1)
	agg := rs.Aggs[0]
	require.Equal(t, "bytes_sent", agg.Key)
	require.Equal(t, int64(42), agg.IntResult)
	require.True(t, agg.HasResult)
}

func TestResolveSplit_AggFloatColumnFromBytes(t *testing.T) {
	reg := newTestRegistry(t)

	row := &SplitRow{
		Values: map[string]any{
			"agg_f04": []byte("1.23"), // latency, FLOAT, AVG
		},
	}

	rs := ResolveSplit(row, reg)

	require.Len(t, rs.Aggs, 1)
	agg := rs.Aggs[0]
	require.Equal(t, "latency", agg.Key)
	require.InDelta(t, 1.23, agg.FloatResult, 1e-9)
	require.True(t, agg.HasResult)
}

func TestResolveSplit_AggInvalidFloatNoResult(t *testing.T) {
	reg := newTestRegistry(t)

	row := &SplitRow{
		Values: map[string]any{
			"agg_f02": "not-a-float", // FLOAT type but unparseable
		},
	}

	rs := ResolveSplit(row, reg)

	require.Len(t, rs.Aggs, 1)
	require.False(t, rs.Aggs[0].HasResult)
}

func TestResolveSplit_AggInvalidIntNoResult(t *testing.T) {
	reg := newTestRegistry(t)

	row := &SplitRow{
		Values: map[string]any{
			"agg_f01": "not-an-int", // INT type but unparseable
		},
	}

	rs := ResolveSplit(row, reg)

	require.Len(t, rs.Aggs, 1)
	require.False(t, rs.Aggs[0].HasResult)
}

func TestResolveSplit_MultipleAggs(t *testing.T) {
	reg := newTestRegistry(t)

	row := &SplitRow{
		Values: map[string]any{
			"agg_f01": int64(404),    // status_code, INT, EQ
			"agg_f02": float64(150),  // response_time.p99, FLOAT, GTE
			"agg_f03": int64(100000), // bytes_sent, INT, SUM
		},
	}

	rs := ResolveSplit(row, reg)

	require.Len(t, rs.Aggs, 3)

	// Index into results by key for stable assertions.
	byKey := make(map[string]ResolvedAgg, len(rs.Aggs))
	for _, a := range rs.Aggs {
		byKey[a.Key] = a
	}

	require.Equal(t, int64(404), byKey["status_code"].IntResult)
	require.InDelta(t, float64(150), byKey["response_time"].FloatResult, 1e-9)
	require.Equal(t, int64(100000), byKey["bytes_sent"].IntResult)
}

func TestResolveSplit_MixedFileAndDimAndAgg(t *testing.T) {
	reg := newTestRegistry(t)

	row := &SplitRow{
		Values: map[string]any{
			metastore.ColMinTimestamp: int64(500),
			metastore.ColState:        "IR_CLOSED",
			"dim_f01":                 "eu-central-1",
			"agg_f01":                 int64(200),
		},
	}

	rs := ResolveSplit(row, reg)

	require.Equal(t, int64(500), rs.File.MinTimestamp)
	require.Equal(t, "IR_CLOSED", rs.File.State)
	require.Equal(t, "eu-central-1", rs.Dimensions["region"])
	require.Len(t, rs.Aggs, 1)
	require.Equal(t, int64(200), rs.Aggs[0].IntResult)
}
