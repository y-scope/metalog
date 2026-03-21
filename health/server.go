package health

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// ReadinessChecker performs a deep health check for the readiness probe.
// Implementations should be fast (< 2s) and safe to call concurrently.
type ReadinessChecker interface {
	CheckReady(ctx context.Context) error
}

// Server provides an HTTP health check endpoint.
type Server struct {
	srv      *http.Server
	ready    atomic.Bool
	checkers []ReadinessChecker
	mu       sync.RWMutex
	log      *zap.Logger
}

// NewServer creates a health server on the given port.
func NewServer(port int, log *zap.Logger) *Server {
	s := &Server{log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/health/live", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/health/ready", s.handleReady)
	s.srv = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s
}

// AddChecker registers a readiness checker that is evaluated on every
// readiness probe. Checkers are called sequentially; the first failure
// makes the probe return 503.
func (s *Server) AddChecker(c ReadinessChecker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkers = append(s.checkers, c)
}

// SetReady marks the server as ready to serve traffic.
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

// Start begins listening. Blocks until the server is stopped.
func (s *Server) Start() error {
	s.log.Info("health server starting", zap.String("addr", s.srv.Addr))
	err := s.srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Stop gracefully shuts down the health server.
func (s *Server) Stop(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK")
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "NOT READY")
		return
	}

	// Run deep health checks with a short timeout.
	s.mu.RLock()
	checkers := s.checkers
	s.mu.RUnlock()

	if len(checkers) > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		for _, c := range checkers {
			if err := c.CheckReady(ctx); err != nil {
				s.log.Warn("readiness check failed", zap.Error(err))
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, "NOT READY")
				return
			}
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "READY")
}
