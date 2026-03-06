package storage

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
)

// mockBackend is a simple mock for testing registry
type mockBackend struct {
	name string
}

func (m *mockBackend) Get(_ context.Context, _, _ string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockBackend) Put(_ context.Context, _, _ string, _ io.Reader, _ int64) error {
	return nil
}
func (m *mockBackend) Delete(_ context.Context, _, _ string) error {
	return nil
}
func (m *mockBackend) Exists(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	backend := &mockBackend{name: "test"}

	r.Register("s3", backend)

	got, err := r.Get("s3")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != backend {
		t.Error("Get() returned different instance")
	}
}

func TestRegistry_Get_NotFound(t *testing.T) {
	r := NewRegistry()

	_, err := r.Get("nonexistent")
	if err == nil {
		t.Error("Get(nonexistent) error = nil, want error")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error should contain backend name, got: %v", err)
	}
}

func TestRegistry_MultipleBackends(t *testing.T) {
	r := NewRegistry()
	s3 := &mockBackend{name: "s3"}
	fs := &mockBackend{name: "fs"}
	gcs := &mockBackend{name: "gcs"}

	r.Register("s3", s3)
	r.Register("filesystem", fs)
	r.Register("gcs", gcs)

	// Verify each
	got, _ := r.Get("s3")
	if got != s3 {
		t.Error("s3 backend mismatch")
	}

	got, _ = r.Get("filesystem")
	if got != fs {
		t.Error("filesystem backend mismatch")
	}

	got, _ = r.Get("gcs")
	if got != gcs {
		t.Error("gcs backend mismatch")
	}
}

func TestRegistry_Overwrite(t *testing.T) {
	r := NewRegistry()
	original := &mockBackend{name: "original"}
	replacement := &mockBackend{name: "replacement"}

	r.Register("s3", original)
	r.Register("s3", replacement)

	got, _ := r.Get("s3")
	if got != replacement {
		t.Error("Get() should return replacement after overwrite")
	}
}

func TestRegistry_Concurrent(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	const goroutines = 100

	// Concurrent Register
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			backend := &mockBackend{name: string(rune('A' + id))}
			r.Register(string(rune('A'+id)), backend)
		}(i)
	}
	wg.Wait()

	// Concurrent Get
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r.Get(string(rune('A' + id)))
		}(i)
	}
	wg.Wait()
}
