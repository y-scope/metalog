package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- CollectSketchValues tests ---

func TestCollectSketchValues_Empty(t *testing.T) {
	preds := CollectSketchValues("", []string{"uuid"}, nil)
	assert.Nil(t, preds)
}

func TestCollectSketchValues_NoFields(t *testing.T) {
	preds := CollectSketchValues("min_timestamp > 1000", nil, nil)
	assert.Nil(t, preds)
}

func TestCollectSketchValues_NoMatchingPredicates(t *testing.T) {
	preds := CollectSketchValues("min_timestamp > 1000 AND state = 'IR_CLOSED'", []string{"uuid"}, nil)
	assert.Nil(t, preds)
}

func TestCollectSketchValues_SingleEquality(t *testing.T) {
	preds := CollectSketchValues("uuid = 'abc-123'", []string{"uuid"}, nil)
	require.Len(t, preds, 1)
	assert.Equal(t, "uuid", preds[0].SketchKey)
	assert.Equal(t, "abc-123", preds[0].Value)
}

func TestCollectSketchValues_MixedAcceleratedAndRegular(t *testing.T) {
	preds := CollectSketchValues("uuid = 'abc' AND min_timestamp > 1000", []string{"uuid"}, nil)
	require.Len(t, preds, 1)
	assert.Equal(t, "uuid", preds[0].SketchKey)
	assert.Equal(t, "abc", preds[0].Value)
}

func TestCollectSketchValues_MultipleAcceleratedFields(t *testing.T) {
	preds := CollectSketchValues("uuid = 'val1' AND trace_id = 'val2'", []string{"uuid", "trace_id"}, nil)
	require.Len(t, preds, 2)

	cols := map[string]string{}
	for _, p := range preds {
		cols[p.SketchKey] = p.Value
	}
	assert.Equal(t, "val1", cols["uuid"])
	assert.Equal(t, "val2", cols["trace_id"])
}

func TestCollectSketchValues_NonEqualityIgnored(t *testing.T) {
	preds := CollectSketchValues("uuid > 'value'", []string{"uuid"}, nil)
	assert.Nil(t, preds)
}

func TestCollectSketchValues_OrNotCollected(t *testing.T) {
	// Predicates inside OR must NOT be collected — pruning based on one side
	// of an OR would incorrectly reject rows matching the other side.
	preds := CollectSketchValues("uuid = 'abc' OR state = 'IR_CLOSED'", []string{"uuid"}, nil)
	assert.Nil(t, preds)
}

func TestCollectSketchValues_NonAcceleratedFieldNotCollected(t *testing.T) {
	preds := CollectSketchValues("uuid = 'abc' AND min_timestamp > 1000", []string{"trace_id"}, nil)
	assert.Nil(t, preds)
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
