package query

import (
	"github.com/axiomhq/splitblockbloom"
	"github.com/cespare/xxhash/v2"

	"github.com/y-scope/metalog/internal/encoding"
)

// sbbfContains checks whether a value is present in a Parquet-compatible Split
// Block Bloom Filter. filterData is the raw block bytes (32 bytes per block).
// The value is hashed with xxHash64 (seed 0), matching the ingestion path in
// clp-ffi-go.
func sbbfContains(filterData []byte, value string) bool {
	if len(filterData) == 0 || len(filterData)%32 != 0 {
		return false
	}
	numBlocks := len(filterData) / 32
	filter := make(splitblockbloom.Filter, numBlocks)
	for i := range filter {
		offset := i * 32
		if err := filter[i].UnmarshalBinary(filterData[offset : offset+32]); err != nil {
			return true // can't deserialize → don't prune
		}
	}
	return filter.Contains(xxhash.Sum64String(value))
}

// extSketchPayload is the decoded structure of the ext MEDIUMBLOB column.
// Format: LZ4( msgpack({ "sketches": { "<key>": { "type": "...", "data": <bytes> }, ... } }) )
type extSketchPayload struct {
	Sketches map[string]struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	} `msgpack:"sketches"`
}

// decodeExtBlob decompresses an LZ4 ext blob and decodes the msgpack payload.
// Uses the shared encoding package which enforces a 16 MB decompression limit.
func decodeExtBlob(compressed []byte) (*extSketchPayload, error) {
	var payload extSketchPayload
	if err := encoding.Unmarshal(compressed, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// evaluateSketchFromExt checks bloom filter membership for a value against
// the decoded ext blob. Returns true if the sketch says the value may be present.
// Returns true (pass-through) if the sketch key is not found in the payload.
func evaluateSketchFromExt(ext *extSketchPayload, sketchKey, value string) bool {
	entry, ok := ext.Sketches[sketchKey]
	if !ok {
		return true // no sketch for this key → can't prune
	}
	if entry.Type != "parquet_sbbf_xxhash64" {
		return true // unknown type → can't evaluate, pass through
	}
	return sbbfContains(entry.Data, value)
}
