package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

func init() {
	RegisterType("http", BackendMeta{
		RequiresBucket: true,
		Factory: func(cfg map[string]string) (StorageBackend, error) {
			return NewHTTPBackend(cfg["baseUrl"], 30*time.Second), nil
		},
	})
}

// HTTPBackend implements a read-only StorageBackend that fetches objects via HTTP(S).
// Useful for reading from CDNs, public object store endpoints, or HTTP file servers.
type HTTPBackend struct {
	baseURL string
	client  *http.Client
}

// NewHTTPBackend creates an HTTPBackend. The baseURL should include the scheme
// (e.g., "https://cdn.example.com/data").
func NewHTTPBackend(baseURL string, timeout time.Duration) *HTTPBackend {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &HTTPBackend{
		baseURL: baseURL,
		client:  &http.Client{Timeout: timeout},
	}
}

// Get retrieves an object via HTTP GET. The bucket is used as a path prefix.
func (b *HTTPBackend) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/%s/%s", b.baseURL, url.PathEscape(bucket), url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, &StorageError{Op: "get", Bucket: bucket, Key: key, Err: err}
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, &StorageError{Op: "get", Bucket: bucket, Key: key, Err: err}
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrObjectNotFound
	}
	if resp.StatusCode == http.StatusForbidden {
		resp.Body.Close()
		return nil, &StorageError{Op: "get", Bucket: bucket, Key: key, Err: ErrAccessDenied}
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, &StorageError{Op: "get", Bucket: bucket, Key: key,
			Err: fmt.Errorf("unexpected status: %d", resp.StatusCode)}
	}

	return resp.Body, nil
}

// Put is not supported on a read-only HTTP backend.
func (b *HTTPBackend) Put(_ context.Context, bucket, key string, _ io.Reader, _ int64) error {
	return &StorageError{Op: "put", Bucket: bucket, Key: key,
		Err: fmt.Errorf("HTTP backend is read-only")}
}

// Delete is not supported on a read-only HTTP backend.
func (b *HTTPBackend) Delete(_ context.Context, bucket, key string) error {
	return &StorageError{Op: "delete", Bucket: bucket, Key: key,
		Err: fmt.Errorf("HTTP backend is read-only")}
}

// Exists checks if an object exists via HTTP HEAD.
func (b *HTTPBackend) Exists(ctx context.Context, bucket, key string) (bool, error) {
	url := fmt.Sprintf("%s/%s/%s", b.baseURL, url.PathEscape(bucket), url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, &StorageError{Op: "exists", Bucket: bucket, Key: key, Err: err}
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return false, &StorageError{Op: "exists", Bucket: bucket, Key: key, Err: err}
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	return false, &StorageError{Op: "exists", Bucket: bucket, Key: key,
		Err: fmt.Errorf("unexpected status: %d", resp.StatusCode)}
}
