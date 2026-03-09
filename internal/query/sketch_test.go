package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- hashToBucket tests ---

func TestHashToBucket_Deterministic(t *testing.T) {
	a := hashToBucket("test_value", 32)
	b := hashToBucket("test_value", 32)
	if a != b {
		t.Errorf("hash not deterministic: %d != %d", a, b)
	}
}

func TestHashToBucket_InRange(t *testing.T) {
	for _, v := range []string{"", "a", "hello", "test_value_123", "日本語"} {
		bucket := hashToBucket(v, 32)
		if bucket < 0 || bucket >= 32 {
			t.Errorf("hashToBucket(%q, 32) = %d, want [0,32)", v, bucket)
		}
	}
}

func TestHashToBucket_Distribution(t *testing.T) {
	// Check that different values produce different buckets (probabilistic)
	seen := make(map[int]bool)
	for i := 0; i < 100; i++ {
		v := string(rune('a'+i%26)) + string(rune('0'+i/26))
		seen[hashToBucket(v, 32)] = true
	}
	// With 100 distinct values and 32 buckets, we should hit at least 15 distinct buckets
	if len(seen) < 15 {
		t.Errorf("poor distribution: only %d of 32 buckets used", len(seen))
	}
}

// --- ExtractSketchPredicates tests ---

func TestExtractSketchPredicates_Empty(t *testing.T) {
	preds, remaining, err := ExtractSketchPredicates("")
	require.NoError(t, err)
	assert.Nil(t, preds)
	assert.Equal(t, "", remaining)
}

func TestExtractSketchPredicates_NoSketchTerms(t *testing.T) {
	expr := "min_timestamp > 1000 AND state = 'IR_CLOSED'"
	preds, remaining, err := ExtractSketchPredicates(expr)
	require.NoError(t, err)
	assert.Nil(t, preds)
	assert.Equal(t, expr, remaining)
}

func TestExtractSketchPredicates_SingleEquality(t *testing.T) {
	expr := "__SKETCH.bloom_col = 'search_term'"
	preds, remaining, err := ExtractSketchPredicates(expr)
	require.NoError(t, err)
	require.Len(t, preds, 1)
	assert.Equal(t, "bloom_col", preds[0].SketchColumn)
	assert.Equal(t, "search_term", preds[0].Value)
	assert.Equal(t, "", remaining)
}

func TestExtractSketchPredicates_MixedSketchAndRegular(t *testing.T) {
	expr := "__SKETCH.bloom_col = 'term' AND min_timestamp > 1000"
	preds, remaining, err := ExtractSketchPredicates(expr)
	require.NoError(t, err)
	require.Len(t, preds, 1)
	assert.Equal(t, "bloom_col", preds[0].SketchColumn)
	assert.Equal(t, "term", preds[0].Value)
	assert.Contains(t, remaining, "min_timestamp")
	assert.NotContains(t, remaining, "__SKETCH")
}

func TestExtractSketchPredicates_MultipleSketchPredicates(t *testing.T) {
	expr := "__SKETCH.col1 = 'val1' AND __SKETCH.col2 = 'val2'"
	preds, remaining, err := ExtractSketchPredicates(expr)
	require.NoError(t, err)
	require.Len(t, preds, 2)
	assert.Equal(t, "", remaining)

	// Verify both predicates were extracted (order may vary due to AST traversal)
	cols := map[string]string{}
	for _, p := range preds {
		cols[p.SketchColumn] = p.Value
	}
	assert.Equal(t, "val1", cols["col1"])
	assert.Equal(t, "val2", cols["col2"])
}

func TestExtractSketchPredicates_SketchBetweenRegularTerms(t *testing.T) {
	expr := "state = 'IR_CLOSED' AND __SKETCH.bloom = 'term' AND min_timestamp > 1000"
	preds, remaining, err := ExtractSketchPredicates(expr)
	require.NoError(t, err)
	require.Len(t, preds, 1)
	assert.Equal(t, "bloom", preds[0].SketchColumn)
	assert.Contains(t, remaining, "state")
	assert.Contains(t, remaining, "min_timestamp")
	assert.NotContains(t, remaining, "__SKETCH")
}

func TestExtractSketchPredicates_NonEqualityIgnored(t *testing.T) {
	// > operator with __SKETCH should NOT be extracted (only = is supported)
	expr := "__SKETCH.bloom_col > 'value'"
	preds, remaining, err := ExtractSketchPredicates(expr)
	require.NoError(t, err)
	assert.Nil(t, preds)
	assert.Equal(t, expr, remaining)
}

func TestExtractSketchPredicates_NonStringValueIgnored(t *testing.T) {
	// Integer value with __SKETCH should NOT be extracted (only string literals)
	expr := "__SKETCH.bloom_col = 123"
	preds, remaining, err := ExtractSketchPredicates(expr)
	require.NoError(t, err)
	assert.Nil(t, preds)
	assert.Equal(t, expr, remaining)
}

func TestExtractSketchPredicates_OrNotExtracted(t *testing.T) {
	// Sketch predicates inside OR should NOT be extracted (would change semantics)
	expr := "__SKETCH.bloom_col = 'term' OR state = 'IR_CLOSED'"
	preds, remaining, err := ExtractSketchPredicates(expr)
	require.NoError(t, err)
	assert.Nil(t, preds)
	assert.Equal(t, expr, remaining)
}

func TestExtractSketchPredicates_UnparseablePassthrough(t *testing.T) {
	// Unparseable expressions should pass through without error
	expr := "AND AND AND"
	preds, remaining, err := ExtractSketchPredicates(expr)
	require.NoError(t, err)
	assert.Nil(t, preds)
	assert.Equal(t, expr, remaining)
}
