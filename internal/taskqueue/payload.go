package taskqueue

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// TaskPayload is the input data for a consolidation task.
// Serialized as LZ4-compressed msgpack in the _task_queue.input column.
type TaskPayload struct {
	TableName      string   `msgpack:"table_name"`
	MinTimestamp   int64    `msgpack:"min_timestamp"`
	FileIDs        []int64  `msgpack:"file_ids"`
	IRPaths        []string `msgpack:"ir_paths"`
	IRBuckets      []string `msgpack:"ir_buckets"`
	IRBackend      string   `msgpack:"ir_backend"`
	ArchivePath    string   `msgpack:"archive_path"`
	ArchiveBucket  string   `msgpack:"archive_bucket"`
	ArchiveBackend string   `msgpack:"archive_backend"`
}

// TaskResult is the output data from a completed consolidation task.
// Serialized as LZ4-compressed msgpack in the _task_queue.output column.
type TaskResult struct {
	ArchivePath      string `msgpack:"archive_path"`
	ArchiveSizeBytes int64  `msgpack:"archive_size_bytes"`
	CreatedAt        int64  `msgpack:"created_at"`
	Error            string `msgpack:"error,omitempty"`
}

// MarshalPayload serializes a TaskPayload to LZ4-compressed msgpack bytes.
func MarshalPayload(p *TaskPayload) ([]byte, error) {
	data, err := compressMsgpack(p)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	return data, nil
}

// UnmarshalPayload deserializes LZ4-compressed msgpack bytes to a TaskPayload.
func UnmarshalPayload(data []byte) (*TaskPayload, error) {
	var p TaskPayload
	if err := decompressMsgpack(data, &p); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}
	return &p, nil
}

// MarshalResult serializes a TaskResult to LZ4-compressed msgpack bytes.
func MarshalResult(r *TaskResult) ([]byte, error) {
	data, err := compressMsgpack(r)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}
	return data, nil
}

// UnmarshalResult deserializes LZ4-compressed msgpack bytes to a TaskResult.
func UnmarshalResult(data []byte) (*TaskResult, error) {
	var r TaskResult
	if err := decompressMsgpack(data, &r); err != nil {
		return nil, fmt.Errorf("unmarshal result: %w", err)
	}
	return &r, nil
}

func compressMsgpack(v any) ([]byte, error) {
	raw, err := msgpack.Marshal(v)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := w.Write(raw); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompressMsgpack(data []byte, v any) error {
	r := lz4.NewReader(bytes.NewReader(data))
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return msgpack.Unmarshal(raw, v)
}
