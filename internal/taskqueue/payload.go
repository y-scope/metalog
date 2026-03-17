package taskqueue

import (
	"fmt"

	"github.com/y-scope/metalog/internal/encoding"
)

// TaskPayloadVersion is the current payload schema version.
// Stored in the _task_queue.version column (not inside the payload blob).
const TaskPayloadVersion = 1

// TaskPayload is the input data for a task.
// Serialized as LZ4-compressed msgpack in the _task_queue.input column.
//
// Top-level fields are task-type-agnostic. Task-specific data lives under
// a typed sub-struct (e.g., Consolidation). The payload version is stored
// as a separate column in _task_queue, not inside the blob.
type TaskPayload struct {
	TableName     string                 `msgpack:"table_name"`
	Consolidation *ConsolidationPayload  `msgpack:"consolidation,omitempty"`
}

// ConsolidationPayload holds the input data specific to a consolidation task.
// Workers MUST write the archive to the specified ArchiveBackend/ArchiveBucket.
// Only the ArchivePath may differ in the result (e.g., if the worker generates
// a different filename), though by default the worker echoes the input path.
type ConsolidationPayload struct {
	MinTimestamp   int64    `msgpack:"min_timestamp"`
	FileIDs        []int64  `msgpack:"file_ids"`
	IRPaths        []string `msgpack:"ir_paths"`
	IRBuckets      []string `msgpack:"ir_buckets"`
	IRBackend      string   `msgpack:"ir_backend"`
	ArchiveBackend string   `msgpack:"archive_backend"`
	ArchiveBucket  string   `msgpack:"archive_bucket"`
	ArchivePath    string   `msgpack:"archive_path"`
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
	data, err := encoding.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	return data, nil
}

// UnmarshalPayload deserializes LZ4-compressed msgpack bytes to a TaskPayload.
// Version validation should be done at the task level (Task.Version column)
// before calling this function.
func UnmarshalPayload(data []byte) (*TaskPayload, error) {
	var p TaskPayload
	if err := encoding.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}
	return &p, nil
}

// MarshalResult serializes a TaskResult to LZ4-compressed msgpack bytes.
func MarshalResult(r *TaskResult) ([]byte, error) {
	data, err := encoding.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}
	return data, nil
}

// UnmarshalResult deserializes LZ4-compressed msgpack bytes to a TaskResult.
func UnmarshalResult(data []byte) (*TaskResult, error) {
	var r TaskResult
	if err := encoding.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("unmarshal result: %w", err)
	}
	return &r, nil
}
