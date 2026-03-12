package taskqueue

import (
	"testing"
)

func TestPayloadRoundTrip(t *testing.T) {
	original := &TaskPayload{
		TableName:      "clp_spark",
		FileIDs:        []int64{1, 2, 3, 4, 5},
		IRPaths:        []string{"/a/b/c.ir", "/d/e/f.ir"},
		IRBuckets:      []string{"logs", "logs"},
		IRBackend:      "s3",
		ArchivePath:    "/archive/out.tar",
		ArchiveBucket:  "archives",
		ArchiveBackend: "s3",
	}

	data, err := MarshalPayload(original)
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnmarshalPayload(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.TableName != original.TableName {
		t.Errorf("TableName = %q, want %q", got.TableName, original.TableName)
	}
	if len(got.FileIDs) != len(original.FileIDs) {
		t.Errorf("len(FileIDs) = %d, want %d", len(got.FileIDs), len(original.FileIDs))
	}
	for i := range got.FileIDs {
		if got.FileIDs[i] != original.FileIDs[i] {
			t.Errorf("FileIDs[%d] = %d, want %d", i, got.FileIDs[i], original.FileIDs[i])
		}
	}
}

func TestResultRoundTrip(t *testing.T) {
	original := &TaskResult{
		ArchivePath:      "/archive/out.tar",
		ArchiveSizeBytes: 1024 * 1024,
		CreatedAt:        1704067200000000000,
	}

	data, err := MarshalResult(original)
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnmarshalResult(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.ArchivePath != original.ArchivePath {
		t.Errorf("ArchivePath = %q, want %q", got.ArchivePath, original.ArchivePath)
	}
	if got.ArchiveSizeBytes != original.ArchiveSizeBytes {
		t.Errorf("ArchiveSizeBytes = %d, want %d", got.ArchiveSizeBytes, original.ArchiveSizeBytes)
	}
}
