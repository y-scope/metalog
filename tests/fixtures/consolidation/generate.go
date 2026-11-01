//go:build ignore

// Run from the repository root: go run tests/fixtures/consolidation/generate.go
package main

import (
    "os"
    "github.com/y-scope/metalog/encoding"
)

// Same named fields/tags as taskqueue/payload.go, without importing its DB dependencies.
type consolidation struct {
    IRBackend string `msgpack:"ir_backend"`
    ArchiveBackend string `msgpack:"archive_backend"`
    ArchiveBucket string `msgpack:"archive_bucket"`
    ArchivePath string `msgpack:"archive_path"`
    FileIDs []int64 `msgpack:"file_ids"`
    IRPaths []string `msgpack:"ir_paths"`
    IRBuckets []string `msgpack:"ir_buckets"`
    MinTimestamp int64 `msgpack:"min_timestamp"`
}
type payload struct {
    Consolidation *consolidation `msgpack:"consolidation,omitempty"`
    TableName string `msgpack:"table_name"`
}
type result struct {
    ArchivePath string `msgpack:"archive_path"`
    Error string `msgpack:"error,omitempty"`
    ArchiveSizeBytes int64 `msgpack:"archive_size_bytes"`
    CreatedAt int64 `msgpack:"created_at"`
}
func write(name string, value any) {
    data, err := encoding.Marshal(value); if err != nil { panic(err) }
    if err = os.WriteFile("tests/fixtures/consolidation/"+name, data, 0644); err != nil { panic(err) }
}
func main() {
    write("go-payload.lz4",payload{&consolidation{"local","local","archives","archives/go",[]int64{1,2},[]string{"one.ir","two.ir"},[]string{"logs"},1234},"go_tasks"})
    write("go-result.lz4",result{ArchivePath:"archives/go",ArchiveSizeBytes:321,CreatedAt:5678})
    write("go-error.lz4",result{Error:"compression failed"})
}
