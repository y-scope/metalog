package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ParseSketchExpression tests ---

func TestParseSketchExpression_Empty(t *testing.T) {
	preds, err := ParseSketchExpression("")
	assert.NoError(t, err)
	assert.Nil(t, preds)
}

func TestParseSketchExpression_SingleEquality(t *testing.T) {
	preds, err := ParseSketchExpression("uuid = 'abc-123'")
	require.NoError(t, err)
	require.Len(t, preds, 1)
	assert.Equal(t, "uuid", preds[0].SketchKey)
	assert.Equal(t, []string{"abc-123"}, preds[0].Values)
}

func TestParseSketchExpression_IN(t *testing.T) {
	preds, err := ParseSketchExpression("uuid IN ('abc', 'def', 'ghi')")
	require.NoError(t, err)
	require.Len(t, preds, 1)
	assert.Equal(t, "uuid", preds[0].SketchKey)
	assert.Equal(t, []string{"abc", "def", "ghi"}, preds[0].Values)
}

func TestParseSketchExpression_MultipleFieldsAND(t *testing.T) {
	preds, err := ParseSketchExpression("uuid = 'val1' AND trace_id = 'val2'")
	require.NoError(t, err)
	require.Len(t, preds, 2)

	keys := map[string][]string{}
	for _, p := range preds {
		keys[p.SketchKey] = p.Values
	}
	assert.Equal(t, []string{"val1"}, keys["uuid"])
	assert.Equal(t, []string{"val2"}, keys["trace_id"])
}

func TestParseSketchExpression_MixedEqualityAndIN(t *testing.T) {
	preds, err := ParseSketchExpression("uuid = 'abc' AND trace_id IN ('x', 'y')")
	require.NoError(t, err)
	require.Len(t, preds, 2)
}

// --- Rejected expressions ---

func TestParseSketchExpression_RejectsOR(t *testing.T) {
	_, err := ParseSketchExpression("uuid = 'abc' OR trace_id = 'def'")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "OR")
}

func TestParseSketchExpression_RejectsNotEqual(t *testing.T) {
	_, err := ParseSketchExpression("uuid != 'abc'")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported operator")
}

func TestParseSketchExpression_RejectsGreaterThan(t *testing.T) {
	_, err := ParseSketchExpression("uuid > 'abc'")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported operator")
}

func TestParseSketchExpression_RejectsLIKE(t *testing.T) {
	_, err := ParseSketchExpression("uuid LIKE '%abc%'")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported operator")
}

func TestParseSketchExpression_RejectsNonStringLiteral(t *testing.T) {
	_, err := ParseSketchExpression("uuid = 123")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "string literal")
}

func TestParseSketchExpression_RejectsInvalidSQL(t *testing.T) {
	_, err := ParseSketchExpression("not valid sql !!!")
	assert.Error(t, err)
}

// --- SBBF Contains tests ---

func TestSbbfContains_EmptyFilter(t *testing.T) {
	assert.False(t, sbbfContains(nil, "test"))
	assert.False(t, sbbfContains([]byte{}, "test"))
}

func TestSbbfContains_InvalidSize(t *testing.T) {
	assert.False(t, sbbfContains(make([]byte, 33), "test"))
}

func TestSbbfContains_AllZeroBlock(t *testing.T) {
	assert.False(t, sbbfContains(make([]byte, 32), "test"))
}

func TestSbbfContains_AllOnesBlock(t *testing.T) {
	block := make([]byte, 32)
	for i := range block {
		block[i] = 0xFF
	}
	assert.True(t, sbbfContains(block, "test"))
	assert.True(t, sbbfContains(block, "anything"))
}

// --- isValidSketchName tests ---

func TestIsValidSketchName(t *testing.T) {
	assert.True(t, isValidSketchName("s01"))
	assert.True(t, isValidSketchName("s64"))
	assert.False(t, isValidSketchName("s00"))
	assert.False(t, isValidSketchName("s65"))
	assert.False(t, isValidSketchName("s99"))
	assert.False(t, isValidSketchName("s1"))
	assert.False(t, isValidSketchName("s001"))
	assert.False(t, isValidSketchName("x01"))
	assert.False(t, isValidSketchName(""))
}
