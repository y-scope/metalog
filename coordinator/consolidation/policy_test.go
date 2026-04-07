package consolidation

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestGenerateArchivePath(t *testing.T) {
	path := GenerateArchivePath()
	if path == "" {
		t.Fatal("expected non-empty path")
	}
	if !strings.HasSuffix(path, ".clp.zst") {
		t.Errorf("path = %q, want suffix .clp.zst", path)
	}

	// Paths should be unique.
	path2 := GenerateArchivePath()
	if path == path2 {
		t.Errorf("paths should be unique: %q == %q", path, path2)
	}
}

func TestGenerateArchivePath_V7Fallback(t *testing.T) {
	// Replace uuid generator to force fallback to V4.
	orig := uuidNewV7
	defer func() { uuidNewV7 = orig }()
	uuidNewV7 = func() (uuid.UUID, error) {
		return uuid.UUID{}, fmt.Errorf("simulated V7 failure")
	}

	path := GenerateArchivePath()
	if path == "" {
		t.Fatal("expected non-empty path from V4 fallback")
	}
	if !strings.HasSuffix(path, ".clp.zst") {
		t.Errorf("path = %q, want suffix .clp.zst", path)
	}
}
