package health

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"
)

func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port, nil
}

// waitForServer polls the health endpoint until it responds or the timeout expires.
func waitForServer(t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("http://127.0.0.1:%d/health", port)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server on port %d not ready after %v", port, timeout)
}

func TestNewServer(t *testing.T) {
	log := zap.NewNop()
	s := NewServer(8080, log)

	if s == nil {
		t.Fatal("NewServer returned nil")
	}
	if s.srv == nil {
		t.Error("http.Server not initialized")
	}
	if s.srv.Addr != ":8080" {
		t.Errorf("Addr = %q, want %q", s.srv.Addr, ":8080")
	}
}

func TestServer_HealthEndpoint(t *testing.T) {
	log := zap.NewNop()
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("getFreePort() error = %v", err)
	}

	s := NewServer(port, log)

	go s.Start()
	defer s.Stop(context.Background())

	waitForServer(t, port, 5*time.Second)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
	if err != nil {
		t.Fatalf("GET /health error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /health status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "OK" {
		t.Errorf("GET /health body = %q, want %q", body, "OK")
	}
}

func TestServer_ReadyEndpoint_NotReady(t *testing.T) {
	log := zap.NewNop()
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("getFreePort() error = %v", err)
	}

	s := NewServer(port, log)

	go s.Start()
	defer s.Stop(context.Background())
	waitForServer(t, port, 5*time.Second)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/ready", port))
	if err != nil {
		t.Fatalf("GET /ready error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("GET /ready status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "NOT READY" {
		t.Errorf("GET /ready body = %q, want %q", body, "NOT READY")
	}
}

func TestServer_ReadyEndpoint_Ready(t *testing.T) {
	log := zap.NewNop()
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("getFreePort() error = %v", err)
	}

	s := NewServer(port, log)
	s.SetReady(true)

	go s.Start()
	defer s.Stop(context.Background())
	waitForServer(t, port, 5*time.Second)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/ready", port))
	if err != nil {
		t.Fatalf("GET /ready error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /ready status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "READY" {
		t.Errorf("GET /ready body = %q, want %q", body, "READY")
	}
}

func TestServer_SetReadyToggle(t *testing.T) {
	log := zap.NewNop()
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("getFreePort() error = %v", err)
	}

	s := NewServer(port, log)

	go s.Start()
	defer s.Stop(context.Background())
	waitForServer(t, port, 5*time.Second)

	// Initially not ready
	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/ready", port))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Error("should be not ready initially")
	}
	resp.Body.Close()

	// Set ready
	s.SetReady(true)
	resp, _ = http.Get(fmt.Sprintf("http://127.0.0.1:%d/ready", port))
	if resp.StatusCode != http.StatusOK {
		t.Error("should be ready after SetReady(true)")
	}
	resp.Body.Close()

	// Set not ready again
	s.SetReady(false)
	resp, _ = http.Get(fmt.Sprintf("http://127.0.0.1:%d/ready", port))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Error("should be not ready after SetReady(false)")
	}
	resp.Body.Close()
}

func TestServer_Stop(t *testing.T) {
	log := zap.NewNop()
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("getFreePort() error = %v", err)
	}

	s := NewServer(port, log)

	go s.Start()
	waitForServer(t, port, 5*time.Second)

	// Verify server is running
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
	if err != nil {
		t.Fatalf("server not running: %v", err)
	}
	resp.Body.Close()

	// Stop server
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Errorf("Stop() error = %v", err)
	}

	// Verify server is stopped — poll until connection refused
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err = http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
		if err != nil {
			return // stopped successfully
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("server should be stopped")
}
