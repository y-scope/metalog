package encoding

import (
	"errors"
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

func TestMarshal_UnmarshalableType(t *testing.T) {
	ch := make(chan int)
	_, err := Marshal(ch)
	if err == nil {
		t.Error("expected error for unmarshable type")
	}
}

func TestUnmarshal_InvalidData(t *testing.T) {
	var s testStruct
	if err := Unmarshal([]byte("not-lz4-data"), &s); err == nil {
		t.Error("expected error for invalid LZ4 data")
	}
}

// failWriter fails on the first Write call.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) {
	return 0, errors.New("disk full")
}

func TestCompressLZ4_WriteError(t *testing.T) {
	err := compressLZ4([]byte("hello"), failWriter{})
	if err == nil {
		t.Error("expected error for failing writer")
	}
}

// limitWriter succeeds on Write but fails on Close (by being too small).
type limitWriter struct {
	n       int
	written int
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.n {
		return 0, errors.New("no space left")
	}
	w.written += len(p)
	return len(p), nil
}

func TestCompressLZ4_CloseError(t *testing.T) {
	err := compressLZ4([]byte("hello world test data"), &limitWriter{n: 20})
	if err == nil {
		t.Error("expected error when writer has insufficient space for LZ4 footer")
	}
}
