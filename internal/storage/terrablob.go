package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

func init() {
	RegisterType("tb", BackendMeta{
		RequiresBucket: false,
		Factory: func(cfg map[string]string) (StorageBackend, error) {
			baseURL := cfg["baseUrl"]
			if baseURL == "" {
				baseURL = "http://localhost:19617"
			}
			return NewTerrablobBackend(baseURL), nil
		},
	})
}

// TerrablobBackend implements StorageBackend using a Terrablob HTTP service.
// Bucket is ignored — URLs are constructed as baseURL/key.
type TerrablobBackend struct {
	baseURL string
	client  *http.Client
}

// NewTerrablobBackend creates a TerrablobBackend.
func NewTerrablobBackend(baseURL string) *TerrablobBackend {
	return &TerrablobBackend{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 60 * time.Second},
	}
}

func (b *TerrablobBackend) Get(ctx context.Context, _, key string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/%s", b.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("tb get: %w", err)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tb get: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrObjectNotFound
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("tb get: unexpected status %d", resp.StatusCode)
	}

	return resp.Body, nil
}

func (b *TerrablobBackend) Put(ctx context.Context, _, key string, body io.Reader, size int64) error {
	url := fmt.Sprintf("%s/%s", b.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return fmt.Errorf("tb put: %w", err)
	}
	req.ContentLength = size

	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("tb put: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("tb put: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (b *TerrablobBackend) Delete(ctx context.Context, _, key string) error {
	url := fmt.Sprintf("%s/%s", b.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("tb delete: %w", err)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("tb delete: %w", err)
	}
	resp.Body.Close()

	// Tolerate 404 on delete
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("tb delete: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (b *TerrablobBackend) Exists(ctx context.Context, _, key string) (bool, error) {
	url := fmt.Sprintf("%s/%s", b.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, fmt.Errorf("tb exists: %w", err)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("tb exists: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	return false, fmt.Errorf("tb exists: unexpected status %d", resp.StatusCode)
}
