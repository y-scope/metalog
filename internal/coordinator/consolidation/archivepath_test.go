package consolidation

import (
	"strings"
	"testing"

	"github.com/y-scope/metalog/internal/taskqueue"
)

func TestArchivePathGenerator_Generate(t *testing.T) {
	gen := NewArchivePathGenerator("archive/")

	payload := &taskqueue.TaskPayload{
		TableName:    "logs_app",
		MinTimestamp: 1704067200000000000, // 2024-01-01T00:00:00Z
	}

	path := gen.Generate(payload, "abc123")

	if !strings.HasPrefix(path, "archive/logs_app/2024/01/01/") {
		t.Errorf("path = %q, want prefix archive/logs_app/2024/01/01/", path)
	}
	if !strings.HasSuffix(path, "/abc123.clp") {
		t.Errorf("path = %q, want suffix /abc123.clp", path)
	}
}

func TestArchivePathGenerator_PrefixNormalization(t *testing.T) {
	gen1 := NewArchivePathGenerator("archive")
	gen2 := NewArchivePathGenerator("archive/")

	if gen1.Prefix != gen2.Prefix {
		t.Errorf("prefix normalization failed: %q != %q", gen1.Prefix, gen2.Prefix)
	}
}

func TestArchivePathGenerator_EmptyPrefix(t *testing.T) {
	gen := NewArchivePathGenerator("")

	payload := &taskqueue.TaskPayload{
		TableName:    "test",
		MinTimestamp: 0,
	}

	path := gen.Generate(payload, "id1")
	if !strings.HasPrefix(path, "test/") {
		t.Errorf("path = %q, want prefix test/", path)
	}
}
