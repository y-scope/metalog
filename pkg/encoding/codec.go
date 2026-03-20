package encoding

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// MaxDecompressedSize is the upper bound on decompressed payload size (16 MB).
// This prevents a crafted LZ4 stream from causing unbounded memory allocation.
const MaxDecompressedSize = 16 << 20

// Marshal serializes v as LZ4-compressed msgpack bytes.
func Marshal(v any) ([]byte, error) {
	raw, err := msgpack.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("msgpack marshal: %w", err)
	}
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := w.Write(raw); err != nil {
		return nil, fmt.Errorf("lz4 write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("lz4 close: %w", err)
	}
	return buf.Bytes(), nil
}

// Unmarshal decompresses LZ4-compressed msgpack bytes into v.
func Unmarshal(data []byte, v any) error {
	r := lz4.NewReader(bytes.NewReader(data))
	raw, err := io.ReadAll(io.LimitReader(r, MaxDecompressedSize+1))
	if err != nil {
		return fmt.Errorf("lz4 decompress: %w", err)
	}
	if len(raw) > MaxDecompressedSize {
		return fmt.Errorf("decompressed payload exceeds %d bytes", MaxDecompressedSize)
	}
	return msgpack.Unmarshal(raw, v)
}
