// Package telemetry provides OpenTelemetry metrics integration for metalog.
//
// The package creates a [metric.MeterProvider] from configuration, supporting
// pluggable exporters (Prometheus, OTLP, or custom). Enterprise deployments
// can register custom exporters via [RegisterExporter] using the same init()
// pattern as storage backends and message transformers.
//
// When telemetry is disabled, subsystems use a no-op meter with zero overhead.
package telemetry
