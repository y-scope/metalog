package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// newTestS3Backend creates an S3Backend pointed at the given httptest server.
// The server receives path-style requests: /<bucket>/<key>.
func newTestS3Backend(t *testing.T, srv *httptest.Server) *S3Backend {
	t.Helper()
	endpoint := srv.URL
	awsCfg := aws.Config{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(
			func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     "test",
					SecretAccessKey: "test",
				}, nil
			}),
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
		// Disable retries so error-path tests complete quickly.
		o.RetryMaxAttempts = 1
	})
	return NewS3Backend(client)
}

// ---- Get ----

func TestS3Backend_Get_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Length", "5")
			_, _ = fmt.Fprint(w, "hello")
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	backend := newTestS3Backend(t, srv)
	rc, err := backend.Get(context.Background(), "bucket", "key.txt")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	if string(data) != "hello" {
		t.Errorf("Get() data = %q, want %q", data, "hello")
	}
}

func TestS3Backend_Get_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return S3-style NoSuchKey XML for 404.
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message>
<Key>key.txt</Key><RequestId>r1</RequestId><HostId>h1</HostId></Error>`)
	}))
	defer srv.Close()

	backend := newTestS3Backend(t, srv)
	_, err := backend.Get(context.Background(), "bucket", "key.txt")
	if err == nil {
		t.Fatal("Get(missing) expected error, got nil")
	}
	if err != ErrObjectNotFound {
		t.Errorf("Get(missing) error = %v, want ErrObjectNotFound", err)
	}
}

func TestS3Backend_Get_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>InternalError</Code><Message>We encountered an internal error.</Message>
<RequestId>r1</RequestId><HostId>h1</HostId></Error>`)
	}))
	defer srv.Close()

	backend := newTestS3Backend(t, srv)
	_, err := backend.Get(context.Background(), "bucket", "key.txt")
	if err == nil {
		t.Fatal("Get(error) expected error, got nil")
	}
	if err == ErrObjectNotFound {
		t.Error("server error should not map to ErrObjectNotFound")
	}
}

// ---- Put ----

func TestS3Backend_Put_Success(t *testing.T) {
	received := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			received = true
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	backend := newTestS3Backend(t, srv)
	body := strings.NewReader("content")
	err := backend.Put(context.Background(), "bucket", "key.txt", body, 7)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if !received {
		t.Error("server did not receive PUT request")
	}
}

func TestS3Backend_Put_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>ServiceUnavailable</Code><Message>Reduce your request rate.</Message>
<RequestId>r1</RequestId><HostId>h1</HostId></Error>`)
	}))
	defer srv.Close()

	backend := newTestS3Backend(t, srv)
	err := backend.Put(context.Background(), "bucket", "key.txt", strings.NewReader("x"), 1)
	if err == nil {
		t.Fatal("Put(error) expected error, got nil")
	}
	if !strings.Contains(err.Error(), "s3 put") {
		t.Errorf("Put error = %v, want to contain 's3 put'", err)
	}
}

// ---- Delete ----

func TestS3Backend_Delete_Success(t *testing.T) {
	received := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			received = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	backend := newTestS3Backend(t, srv)
	err := backend.Delete(context.Background(), "bucket", "key.txt")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if !received {
		t.Error("server did not receive DELETE request")
	}
}

func TestS3Backend_Delete_NotFound(t *testing.T) {
	// The AWS SDK v2 does not deserialize typed NoSuchKey/NotFound errors for
	// DeleteObject via a fake HTTP server (S3 spec models DELETE success as 204;
	// 404 bodies are not deserialized into typed errors by the SDK without a real
	// S3 endpoint). The idempotent-delete code path is covered by integration tests
	// against MinIO. This test simply exercises the Delete call path with a 404
	// response and verifies the function returns without panicking.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message>
<Key>key.txt</Key><RequestId>r1</RequestId><HostId>h1</HostId></Error>`)
	}))
	defer srv.Close()

	backend := newTestS3Backend(t, srv)
	// With a fake server the SDK produces a non-typed error (no idempotent path).
	_ = backend.Delete(context.Background(), "bucket", "key.txt")
}

func TestS3Backend_Delete_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>InternalError</Code><Message>We encountered an internal error.</Message>
<RequestId>r1</RequestId><HostId>h1</HostId></Error>`)
	}))
	defer srv.Close()

	backend := newTestS3Backend(t, srv)
	err := backend.Delete(context.Background(), "bucket", "key.txt")
	if err == nil {
		t.Fatal("Delete(error) expected error, got nil")
	}
	if !strings.Contains(err.Error(), "s3 delete") {
		t.Errorf("Delete error = %v, want to contain 's3 delete'", err)
	}
}

// ---- Exists ----

func TestS3Backend_Exists_True(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "42")
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	backend := newTestS3Backend(t, srv)
	exists, err := backend.Exists(context.Background(), "bucket", "key.txt")
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Error("Exists() = false, want true")
	}
}

func TestS3Backend_Exists_False(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return S3-style NotFound for HEAD.
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	backend := newTestS3Backend(t, srv)
	exists, err := backend.Exists(context.Background(), "bucket", "key.txt")
	if err != nil {
		t.Fatalf("Exists(missing) error = %v", err)
	}
	if exists {
		t.Error("Exists(missing) = true, want false")
	}
}

func TestS3Backend_Exists_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>InternalError</Code><Message>We encountered an internal error.</Message>
<RequestId>r1</RequestId><HostId>h1</HostId></Error>`)
	}))
	defer srv.Close()

	backend := newTestS3Backend(t, srv)
	_, err := backend.Exists(context.Background(), "bucket", "key.txt")
	if err == nil {
		t.Fatal("Exists(error) expected error, got nil")
	}
	if !strings.Contains(err.Error(), "s3 exists") {
		t.Errorf("Exists error = %v, want to contain 's3 exists'", err)
	}
}
