package query

import "testing"

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
