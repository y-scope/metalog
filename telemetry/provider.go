// Package telemetry provides OpenTelemetry metrics integration for metalog.
//
// The package creates a [metric.MeterProvider] from configuration, supporting
// pluggable exporters (Prometheus, OTLP, or custom). Enterprise deployments
// can register custom exporters via [RegisterExporter] using the same init()
// pattern as storage backends and message transformers.
//
// When telemetry is disabled, subsystems use a no-op meter with zero overhead.
package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	prometheusexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// ExporterFactory creates an OTel metric reader from configuration.
type ExporterFactory func(cfg map[string]string) (sdkmetric.Reader, error)

var (
	exporterMu       sync.RWMutex
	exporterRegistry = map[string]ExporterFactory{}
)

// RegisterExporter registers a named exporter factory. Enterprise deployments
// can add custom exporters (e.g., Datadog, M3) via init() in a separate module.
func RegisterExporter(name string, factory ExporterFactory) {
	exporterMu.Lock()
	defer exporterMu.Unlock()
	exporterRegistry[name] = factory
}

// Config holds telemetry configuration.
type Config struct {
	Options  map[string]string `yaml:"options"`
	Exporter string            `yaml:"exporter"`
	Enabled  bool              `yaml:"enabled"`
}

// Provider wraps an OTel MeterProvider with lifecycle management.
type Provider struct {
	provider *sdkmetric.MeterProvider
	handler  http.Handler // Prometheus HTTP handler (nil if not prometheus)
}

// NewProvider creates a Provider from configuration. Returns a no-op provider
// if telemetry is disabled.
func NewProvider(cfg Config) (*Provider, error) {
	if !cfg.Enabled {
		return &Provider{}, nil
	}

	exporter := cfg.Exporter
	if exporter == "" {
		exporter = "prometheus"
	}

	var reader sdkmetric.Reader
	var handler http.Handler

	switch exporter {
	case "prometheus":
		// Use a dedicated registry to avoid conflicts with the global
		// Prometheus registry (safe for tests and multiple providers).
		reg := prometheus.NewRegistry()
		promExporter, err := prometheusexporter.New(
			prometheusexporter.WithRegisterer(reg),
		)
		if err != nil {
			return nil, fmt.Errorf("create prometheus exporter: %w", err)
		}
		reader = promExporter
		handler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	default:
		// Check custom exporter registry
		exporterMu.RLock()
		factory, ok := exporterRegistry[exporter]
		exporterMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("unknown telemetry exporter: %q", exporter)
		}
		var err error
		reader, err = factory(cfg.Options)
		if err != nil {
			return nil, fmt.Errorf("create %s exporter: %w", exporter, err)
		}
	}

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	return &Provider{
		provider: provider,
		handler:  handler,
	}, nil
}

// Meter returns a named Meter for a subsystem. If telemetry is disabled,
// returns a no-op meter.
func (p *Provider) Meter(name string) metric.Meter {
	if p == nil || p.provider == nil {
		return noop.Meter{}
	}
	return p.provider.Meter(name)
}

// Handler returns the Prometheus HTTP handler, or nil if not using Prometheus.
func (p *Provider) Handler() http.Handler {
	if p == nil {
		return nil
	}
	return p.handler
}

// Shutdown flushes and shuts down the meter provider.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.provider == nil {
		return nil
	}
	return p.provider.Shutdown(ctx)
}
