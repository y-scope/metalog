//go:build ignore

// Run from the repository root: go run tests/fixtures/bloom/generate.go
// Uses the same pinned filter/MessagePack/LZ4 libraries as the Go implementation.
package main

import (
	"encoding/binary"
	"fmt"
	"github.com/axiomhq/splitblockbloom"
	"github.com/cespare/xxhash/v2"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/y-scope/metalog/encoding"
	"os"
)

type snapshot struct {
	Type string `msgpack:"type"`
	Data []byte `msgpack:"data"`
}
type payload struct {
	Sketches map[string]snapshot `msgpack:"sketches"`
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func write(name string, data []byte) { check(os.WriteFile("tests/fixtures/bloom/"+name, data, 0644)) }
func main() {
	values := []string{"", "abc", "user-12345", "a'b", "é🙂", "nul\x00value"}
	for _, blocks := range []int{1, 3, 16} {
		f := make(splitblockbloom.Filter, blocks)
		for _, v := range values {
			f.AddHash(xxhash.Sum64String(v))
		}
		if f.Contains(xxhash.Sum64String("definitely-absent")) {
			panic("choose another negative fixture value")
		}
		for _, v := range values {
			if !f.Contains(xxhash.Sum64String(v)) {
				panic("Go false negative")
			}
		}
		// Independent reference encoder for the published Parquet algorithm.
		// The pinned Go library uses modulo block selection and swapped lanes.
		parquet := make([]byte, blocks*32)
		salts := []uint32{0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31}
		for _, v := range values {
			h := xxhash.Sum64String(v)
			offset := int(((h>>32)*uint64(blocks))>>32) * 32
			for lane, salt := range salts {
				word := parquet[offset+lane*4 : offset+lane*4+4]
				binary.LittleEndian.PutUint32(word, binary.LittleEndian.Uint32(word)|(1<<((uint32(h)*salt)>>27)))
			}
		}
		write(fmt.Sprintf("parquet_%d.bin", blocks), parquet)
		var data []byte
		for i := range f {
			data = f[i].AppendTo(data)
		}
		write(fmt.Sprintf("sbbf_%d.bin", blocks), data)
		if blocks == 16 {
			snap := snapshot{"parquet_sbbf_xxhash64", data}
			wire, err := msgpack.Marshal(snap)
			check(err)
			write("snapshot.msgpack", wire)
			ext, err := encoding.Marshal(payload{map[string]snapshot{"trace_id": snap, "user_id": snap}})
			check(err)
			write("ext.lz4", ext)
		}
	}
	// Optionally validate Rust-produced bytes with the independent Go decoder.
	if len(os.Args) > 1 {
		raw, err := os.ReadFile(os.Args[1])
		check(err)
		var p payload
		check(encoding.Unmarshal(raw, &p))
		f := make(splitblockbloom.Filter, len(p.Sketches["trace_id"].Data)/32)
		for i := range f {
			check(f[i].UnmarshalBinary(p.Sketches["trace_id"].Data[i*32 : (i+1)*32]))
		}
		for _, v := range values {
			if !f.Contains(xxhash.Sum64String(v)) {
				panic("Rust->Go false negative")
			}
		}
		fmt.Println("PASS Rust-produced ext decoded and checked by Go")
	}
}
