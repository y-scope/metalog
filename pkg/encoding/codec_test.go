package encoding

import (
	"testing"
)

type testStruct struct {
	Name  string `msgpack:"name"`
	Value int    `msgpack:"value"`
}

func TestMarshalUnmarshal_RoundTrip(t *testing.T) {
	original := testStruct{Name: "hello", Value: 42}

	data, err := Marshal(&original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}

	var decoded testStruct
	if err := Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.Name != original.Name || decoded.Value != original.Value {
		t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, original)
	}
}

func TestUnmarshal_NilData(t *testing.T) {
	var s testStruct
	err := Unmarshal(nil, &s)
	if err == nil {
		t.Error("expected error for nil data")
	}
}
