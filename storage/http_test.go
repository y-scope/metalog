package storage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPBackend_Get(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bucket/key.txt" {
			w.Write([]byte("hello"))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	backend := NewHTTPBackend(srv.URL, 5*time.Second)
	ctx := context.Background()

	rc, err := backend.Get(ctx, "bucket", "key.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	if string(data) != "hello" {
		t.Errorf("got %q, want hello", data)
	}
}

func TestHTTPBackend_Get_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	backend := NewHTTPBackend(srv.URL, 5*time.Second)
	_, err := backend.Get(context.Background(), "bucket", "missing.txt")

	if !IsNotFound(err) {
		t.Errorf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestHTTPBackend_Put_ReadOnly(t *testing.T) {
	backend := NewHTTPBackend("http://example.com", 5*time.Second)
	err := backend.Put(context.Background(), "b", "k", nil, 0)
	if err == nil {
		t.Error("expected error for read-only Put")
	}
}

func TestHTTPBackend_Delete_ReadOnly(t *testing.T) {
	backend := NewHTTPBackend("http://example.com", 5*time.Second)
	err := backend.Delete(context.Background(), "b", "k")
	if err == nil {
		t.Error("expected error for read-only Delete")
	}
}

func TestHTTPBackend_Exists(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/b/exists.txt" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	backend := NewHTTPBackend(srv.URL, 5*time.Second)
	ctx := context.Background()

	exists, err := backend.Exists(ctx, "b", "exists.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("expected true for existing object")
	}

	exists, err = backend.Exists(ctx, "b", "missing.txt")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("expected false for missing object")
	}
}
