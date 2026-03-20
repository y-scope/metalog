// Package health provides a lightweight HTTP server for Kubernetes-style
// liveness and readiness probes. The [Server] exposes /health and /health/live
// (liveness) and /ready and /health/ready (readiness) endpoints with atomic
// ready-state toggling.
package health
