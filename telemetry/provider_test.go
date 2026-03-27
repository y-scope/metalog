package telemetry

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewProvider_Disabled(t *testing.T) {
	p, err := NewProvider(Config{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if p.Handler() != nil {
		t.Error("disabled provider should have nil handler")
	}
	m := p.Meter("test")
	counter, _ := m.Int64Counter("test.counter")
	counter.Add(context.Background(), 1) // no panic on noop
}

func TestNewProvider_Prometheus(t *testing.T) {
	p, err := NewProvider(Config{Enabled: true, Exporter: "prometheus"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown(context.Background())

	if p.Handler() == nil {
		t.Error("prometheus provider should have non-nil handler")
	}
	m := p.Meter("test")
	counter, _ := m.Int64Counter("test.counter")
	counter.Add(context.Background(), 1)
}

func TestNewProvider_DefaultExporter(t *testing.T) {
	p, err := NewProvider(Config{Enabled: true, Exporter: ""})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown(context.Background())

	if p.Handler() == nil {
		t.Error("default exporter should be prometheus with non-nil handler")
	}
}

func TestNewProvider_UnknownExporter(t *testing.T) {
	_, err := NewProvider(Config{Enabled: true, Exporter: "nonexistent"})
	if err == nil {
		t.Error("expected error for unknown exporter")
	}
}

func TestNewProvider_CustomExporter(t *testing.T) {
	RegisterExporter("test_noop", func(cfg map[string]string) (sdkmetric.Reader, error) {
		return sdkmetric.NewPeriodicReader(noopExporter{}), nil
	})

	p, err := NewProvider(Config{Enabled: true, Exporter: "test_noop"})
	if err != nil {
		t.Fatalf("custom exporter should work: %v", err)
	}
	defer p.Shutdown(context.Background())

	if p.Handler() != nil {
		t.Error("custom exporter should have nil handler (not prometheus)")
	}
	m := p.Meter("test")
	counter, _ := m.Int64Counter("test.counter")
	counter.Add(context.Background(), 1)
}

func TestProvider_NilSafe(t *testing.T) {
	var p *Provider
	if err := p.Shutdown(context.Background()); err != nil {
		t.Error("shutdown on nil provider should not error")
	}
	m := p.Meter("test")
	counter, _ := m.Int64Counter("test.counter")
	counter.Add(context.Background(), 1) // no panic
}

// noopExporter satisfies sdkmetric.Exporter with no-op implementations.
type noopExporter struct{}

func (noopExporter) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}
func (noopExporter) Aggregation(sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.AggregationDefault{}
}
func (noopExporter) Export(context.Context, *metricdata.ResourceMetrics) error { return nil }
func (noopExporter) ForceFlush(context.Context) error                         { return nil }
func (noopExporter) Shutdown(context.Context) error                           { return nil }
