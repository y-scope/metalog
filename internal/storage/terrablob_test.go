package storage

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTerrablobBackend_Get(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if r.URL.Path == "/testkey" {
			w.Write([]byte("hello"))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	b := NewTerrablobBackend(srv.URL)
	ctx := context.Background()

	// Successful get
	rc, err := b.Get(ctx, "ignored-bucket", "testkey")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "hello" {
		t.Errorf("Get() = %q, want %q", data, "hello")
	}

	// Not found
	_, err = b.Get(ctx, "", "missing")
	if err != ErrObjectNotFound {
		t.Errorf("Get(missing) error = %v, want ErrObjectNotFound", err)
	}
}

func TestTerrablobBackend_Put(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	b := NewTerrablobBackend(srv.URL)
	body := []byte("data")
	err := b.Put(context.Background(), "", "mykey", bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if !bytes.Equal(gotBody, body) {
		t.Errorf("server received %q, want %q", gotBody, body)
	}
}

func TestTerrablobBackend_Delete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if r.URL.Path == "/exists" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusNotFound) // tolerated
		}
	}))
	defer srv.Close()

	b := NewTerrablobBackend(srv.URL)

	if err := b.Delete(context.Background(), "", "exists"); err != nil {
		t.Errorf("Delete(exists) error = %v", err)
	}
	if err := b.Delete(context.Background(), "", "missing"); err != nil {
		t.Errorf("Delete(missing) error = %v, want nil (404 tolerated)", err)
	}
}

func TestTerrablobBackend_Exists(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("expected HEAD, got %s", r.Method)
		}
		if r.URL.Path == "/present" {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	b := NewTerrablobBackend(srv.URL)
	ctx := context.Background()

	ok, err := b.Exists(ctx, "", "present")
	if err != nil || !ok {
		t.Errorf("Exists(present) = %v, %v; want true, nil", ok, err)
	}

	ok, err = b.Exists(ctx, "", "missing")
	if err != nil || ok {
		t.Errorf("Exists(missing) = %v, %v; want false, nil", ok, err)
	}
}

func TestTerrablobBackend_IgnoresBucket(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	b := NewTerrablobBackend(srv.URL)
	b.Get(context.Background(), "any-bucket", "mykey")
	if gotPath != "/mykey" {
		t.Errorf("path = %q, want /mykey (bucket should be ignored)", gotPath)
	}
}

func TestCreateBackend_Terrablob(t *testing.T) {
	backend, err := CreateBackend("tb", map[string]string{
		"baseUrl": "http://localhost:19617",
	})
	if err != nil {
		t.Fatalf("CreateBackend(tb) error = %v", err)
	}
	if _, ok := backend.(*TerrablobBackend); !ok {
		t.Errorf("expected *TerrablobBackend, got %T", backend)
	}
}

func TestRequiresBucket_Terrablob(t *testing.T) {
	if RequiresBucket("tb") {
		t.Error("tb should not require bucket")
	}
	if !RequiresBucket("s3") {
		t.Error("s3 should require bucket")
	}
}
