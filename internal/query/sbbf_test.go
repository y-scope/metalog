package query

import (
	"bytes"
	"testing"

	"github.com/axiomhq/splitblockbloom"
	"github.com/cespare/xxhash/v2"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// buildTestFilter creates a real SBBF filter with the given values inserted.
func buildTestFilter(values []string, capacity int) []byte {
	bpv := splitblockbloom.RecommendedBitsPerValue(uint64(capacity), 0.01)
	f := splitblockbloom.NewFilter(uint64(capacity), uint64(bpv))
	for _, v := range values {
		f.AddHash(xxhash.Sum64String(v))
	}
	var data []byte
	for i := 0; i < f.NumBlocks(); i++ {
		data = f[i].AppendTo(data)
	}
	return data
}

// buildTestExtBlob builds a complete ext blob (LZ4+msgpack) with one sketch entry.
func buildTestExtBlob(t *testing.T, sketchKey string, filterData []byte) []byte {
	t.Helper()

	type snapshot struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	}
	type extPayload struct {
		Sketches map[string]*snapshot `msgpack:"sketches"`
	}
	payload := &extPayload{
		Sketches: map[string]*snapshot{
			sketchKey: {
				Type: "parquet_sbbf_xxhash64",
				Data: filterData,
			},
		},
	}

	mpData, err := msgpack.Marshal(payload)
	require.NoError(t, err)

	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	_, err = w.Write(mpData)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.Bytes()
}

func TestDecodeExtBlob_RoundTrip(t *testing.T) {
	filterData := buildTestFilter([]string{"abc", "def"}, 128)
	blob := buildTestExtBlob(t, "uuid", filterData)

	ext, err := decodeExtBlob(blob)
	require.NoError(t, err)
	require.Contains(t, ext.Sketches, "uuid")
	assert.Equal(t, "parquet_sbbf_xxhash64", ext.Sketches["uuid"].Type)
	assert.Equal(t, filterData, ext.Sketches["uuid"].Data)
}

func TestDecodeExtBlob_InvalidLZ4(t *testing.T) {
	_, err := decodeExtBlob([]byte("not lz4"))
	assert.Error(t, err)
}

func TestEvaluateSketchFromExt_Match(t *testing.T) {
	filterData := buildTestFilter([]string{"abc-123", "def-456"}, 128)
	blob := buildTestExtBlob(t, "uuid", filterData)

	ext, err := decodeExtBlob(blob)
	require.NoError(t, err)

	assert.True(t, evaluateSketchFromExt(ext, "uuid", "abc-123"))
	assert.True(t, evaluateSketchFromExt(ext, "uuid", "def-456"))
}

func TestEvaluateSketchFromExt_NoMatch(t *testing.T) {
	filterData := buildTestFilter([]string{"abc-123"}, 128)
	blob := buildTestExtBlob(t, "uuid", filterData)

	ext, err := decodeExtBlob(blob)
	require.NoError(t, err)

	assert.False(t, evaluateSketchFromExt(ext, "uuid", "not-inserted"))
}

func TestEvaluateSketchFromExt_MissingKey(t *testing.T) {
	filterData := buildTestFilter([]string{"abc"}, 128)
	blob := buildTestExtBlob(t, "uuid", filterData)

	ext, err := decodeExtBlob(blob)
	require.NoError(t, err)

	// Key not in ext → pass through (can't prune)
	assert.True(t, evaluateSketchFromExt(ext, "trace_id", "abc"))
}

func TestEvaluateSketchFromExt_UnknownType(t *testing.T) {
	// Manually build ext with unknown type
	type snapshot struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	}
	type extPayload struct {
		Sketches map[string]*snapshot `msgpack:"sketches"`
	}
	payload := &extPayload{
		Sketches: map[string]*snapshot{
			"uuid": {Type: "unknown_filter_type", Data: []byte{1, 2, 3}},
		},
	}
	mpData, err := msgpack.Marshal(payload)
	require.NoError(t, err)

	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	_, err = w.Write(mpData)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	ext, err := decodeExtBlob(buf.Bytes())
	require.NoError(t, err)

	// Unknown type → pass through
	assert.True(t, evaluateSketchFromExt(ext, "uuid", "anything"))
}

func TestEvaluateSketchPredicatesFromRow_NilExt(t *testing.T) {
	preds := []SketchPredicate{{SketchKey: "uuid", Value: "abc"}}
	pass, err := evaluateSketchPredicatesFromRow(nil, preds)
	assert.NoError(t, err)
	assert.True(t, pass) // no ext → pass through
}

func TestEvaluateSketchPredicatesFromRow_Match(t *testing.T) {
	filterData := buildTestFilter([]string{"abc-123"}, 128)
	blob := buildTestExtBlob(t, "uuid", filterData)

	preds := []SketchPredicate{{SketchKey: "uuid", Value: "abc-123"}}
	pass, err := evaluateSketchPredicatesFromRow(blob, preds)
	assert.NoError(t, err)
	assert.True(t, pass)
}

func TestEvaluateSketchPredicatesFromRow_Prune(t *testing.T) {
	filterData := buildTestFilter([]string{"abc-123"}, 128)
	blob := buildTestExtBlob(t, "uuid", filterData)

	preds := []SketchPredicate{{SketchKey: "uuid", Value: "not-present"}}
	pass, err := evaluateSketchPredicatesFromRow(blob, preds)
	assert.NoError(t, err)
	assert.False(t, pass) // bloom filter says definitely not → prune
}

func TestEvaluateSketchPredicatesFromRow_MultiplePredicates(t *testing.T) {
	// Build ext with two sketch entries
	uuidFilter := buildTestFilter([]string{"abc"}, 128)
	traceFilter := buildTestFilter([]string{"xyz"}, 128)

	type snapshot struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	}
	type extPayload struct {
		Sketches map[string]*snapshot `msgpack:"sketches"`
	}
	payload := &extPayload{
		Sketches: map[string]*snapshot{
			"uuid":     {Type: "parquet_sbbf_xxhash64", Data: uuidFilter},
			"trace_id": {Type: "parquet_sbbf_xxhash64", Data: traceFilter},
		},
	}
	mpData, err := msgpack.Marshal(payload)
	require.NoError(t, err)

	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	_, err = w.Write(mpData)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	blob := buf.Bytes()

	// Both match → pass
	preds := []SketchPredicate{
		{SketchKey: "uuid", Value: "abc"},
		{SketchKey: "trace_id", Value: "xyz"},
	}
	pass, err := evaluateSketchPredicatesFromRow(blob, preds)
	assert.NoError(t, err)
	assert.True(t, pass)

	// One doesn't match → prune
	preds = []SketchPredicate{
		{SketchKey: "uuid", Value: "abc"},
		{SketchKey: "trace_id", Value: "not-present"},
	}
	pass, err = evaluateSketchPredicatesFromRow(blob, preds)
	assert.NoError(t, err)
	assert.False(t, pass)
}

func TestSbbfContains_RealFilter(t *testing.T) {
	filterData := buildTestFilter([]string{"hello", "world", "test-value-123"}, 256)

	assert.True(t, sbbfContains(filterData, "hello"))
	assert.True(t, sbbfContains(filterData, "world"))
	assert.True(t, sbbfContains(filterData, "test-value-123"))
	assert.False(t, sbbfContains(filterData, "not-inserted"))
}
