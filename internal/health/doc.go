// Package health provides a lightweight HTTP server for Kubernetes-style
// liveness and readiness probes. The [Server] exposes /healthz (liveness)
// and /readyz (readiness) endpoints with atomic ready-state toggling.
package health
