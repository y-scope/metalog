package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// loadYAML writes YAML to a temp file and loads it.
func loadYAML(t *testing.T, yaml string) (*NodeConfig, error) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "node.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	return LoadNodeConfig(path)
}

// validPrimary is a minimal valid primary DB config block.
const validPrimary = `
database:
  primary:
    host: localhost
    port: 3306
    database: test
    user: root
    password: pass
`

const validReplica = `
database:
  replica:
    host: replica-db
    port: 3306
    database: test
    user: reader
    password: pass
`

func TestLoadNodeConfig(t *testing.T) {
	cfg, err := loadYAML(t, validPrimary+`
coordinator:
  enabled: true
  name: test-node
  haStrategy: heartbeat
worker:
  concurrency: 4
  clpBinaryPath: /usr/bin/clp-s
`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Coordinator.Name != "test-node" || !cfg.HasCoordinator() {
		t.Errorf("unexpected coordinator state: name=%q enabled=%v", cfg.Coordinator.Name, cfg.HasCoordinator())
	}
	if eff := cfg.EffectiveReplica(); eff.Host != "localhost" {
		t.Errorf("EffectiveReplica().Host = %q, want localhost (fallback)", eff.Host)
	}
}

func TestLoadNodeConfig_WithReplica(t *testing.T) {
	cfg, err := loadYAML(t, `
database:
  primary:
    host: primary-db
    port: 3306
    database: test
    user: root
    password: pass
  replica:
    host: replica-db
    port: 3307
    database: test
    user: reader
    password: secret
coordinator:
  enabled: true
`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Replica == nil || cfg.Database.Replica.Host != "replica-db" {
		t.Error("expected replica-db")
	}
	if eff := cfg.EffectiveReplica(); eff.Host != "replica-db" {
		t.Errorf("EffectiveReplica().Host = %q, want replica-db", eff.Host)
	}
}

func TestLoadNodeConfig_GRPCQueryOnlyWithReplica(t *testing.T) {
	cfg, err := loadYAML(t, validReplica+"grpc:\n  port: 9090\n  query: true\n  metadata: true")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.GRPC.Query || !cfg.GRPC.Metadata || cfg.HasCoordinator() {
		t.Error("expected query+metadata enabled, no coordinator")
	}
}

func TestLoadNodeConfig_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{"no database", "coordinator:\n  enabled: false"},
		{"invalid primary port", "database:\n  primary:\n    host: h\n    port: 99999\n    database: d\n    user: u\n    password: p"},
		{"invalid replica port", "database:\n  primary:\n    host: h\n    port: 3306\n    database: d\n    user: u\n    password: p\n  replica:\n    host: r\n    port: 0\n    database: d\n    user: u\n    password: p"},
		{"invalid health port", validPrimary + "health:\n  enabled: true\n  port: 99999"},
		{"invalid grpc port", validPrimary + "grpc:\n  port: 99999\n  query: true"},
		{"invalid HA strategy", validPrimary + "coordinator:\n  enabled: true\n  haStrategy: invalid"},
		{"lease renewal >= TTL", validPrimary + "coordinator:\n  enabled: true\n  haStrategy: lease\n  leaseTtlSeconds: 60\n  leaseRenewalIntervalSeconds: 60"},
		{"negative lease TTL", validPrimary + "coordinator:\n  enabled: true\n  haStrategy: lease\n  leaseTtlSeconds: -1"},
		{"negative lease renewal", validPrimary + "coordinator:\n  enabled: true\n  haStrategy: lease\n  leaseRenewalIntervalSeconds: -1"},
		{"negative heartbeat", validPrimary + "coordinator:\n  enabled: true\n  heartbeatIntervalSeconds: -1"},
		{"negative dead node threshold", validPrimary + "coordinator:\n  enabled: true\n  deadNodeThresholdSeconds: -1"},
		{"negative reconciliation", validPrimary + "coordinator:\n  enabled: true\n  reconciliationIntervalSeconds: -1"},
		{"ingestion without coordinator", validPrimary + "grpc:\n  ingestion: true"},
		{"invalid default backend", validPrimary + "storage:\n  defaultBackend: nonexistent\n  backends:\n    minio:\n      endpoint: http://minio:9000\ncoordinator:\n  enabled: true"},
		{"replica-only rejects coordinator", validReplica + "coordinator:\n  enabled: true"},
		{"admin without primary", validReplica + "grpc:\n  port: 9090\n  admin: true"},
		{"invalid YAML", "{{{{not yaml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := loadYAML(t, tt.yaml); err == nil {
				t.Error("expected error")
			}
		})
	}
	t.Run("file not found", func(t *testing.T) {
		if _, err := LoadNodeConfig("/nonexistent/path"); err == nil {
			t.Error("expected error")
		}
	})
}

func TestLoadNodeConfig_WorkerWithoutClpBinary(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	if _, err := loadYAML(t, validPrimary+"worker:\n  concurrency: 2"); err == nil {
		t.Fatal("expected error when no clp binary")
	}
}

func TestDSN(t *testing.T) {
	cfg := DatabaseConfig{Host: "localhost", Port: 3306, Database: "metalog", User: "root", Password: "pw"}
	want := "root:pw@tcp(localhost:3306)/metalog?interpolateParams=true&parseTime=true&maxAllowedPacket=16777216"
	if got := cfg.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
	cfg.Port = 0
	if got := cfg.DSN(); !strings.Contains(got, "localhost:3306") {
		t.Errorf("DSN() with port=0: %q, expected default port 3306", got)
	}
}

func TestFailureLogInterval(t *testing.T) {
	for _, tt := range []struct{ secs int; want time.Duration }{
		{0, 60 * time.Second}, {-5, 60 * time.Second}, {30, 30 * time.Second},
	} {
		if got := (&LoggingConfig{FailureLogIntervalSeconds: tt.secs}).FailureLogInterval(); got != tt.want {
			t.Errorf("FailureLogInterval(%d) = %v, want %v", tt.secs, got, tt.want)
		}
	}
}

func TestIsBlockingIngestion(t *testing.T) {
	f, tr := false, true
	for _, tt := range []struct{ ptr *bool; want bool }{
		{nil, true}, {&f, false}, {&tr, true},
	} {
		if got := (&GRPCConfig{BlockingIngestion: tt.ptr}).IsBlockingIngestion(); got != tt.want {
			t.Errorf("IsBlockingIngestion(%v) = %v, want %v", tt.ptr, got, tt.want)
		}
	}
}

func TestHasAnyService(t *testing.T) {
	none := GRPCConfig{}
	if none.HasAnyService() {
		t.Error("empty config should have no services")
	}
	withQuery := GRPCConfig{Query: true}
	if !withQuery.HasAnyService() {
		t.Error("config with query should have a service")
	}
}

func TestResolveNodeID(t *testing.T) {
	t.Setenv("MY_NODE_ID", "node-42")
	cfg := NodeConfig{Coordinator: CoordinatorConfig{NodeIDEnvVar: "MY_NODE_ID"}}
	if got, err := cfg.ResolveNodeID(); err != nil || got != "node-42" {
		t.Errorf("ResolveNodeID() = %q, %v; want node-42", got, err)
	}

	hostname, _ := os.Hostname()
	if got, err := (&NodeConfig{}).ResolveNodeID(); err != nil || got != hostname {
		t.Errorf("ResolveNodeID() = %q, %v; want %q", got, err, hostname)
	}
}

func TestStorageBackendConfig_ToMap(t *testing.T) {
	cfg := StorageBackendConfig{
		Endpoint: "http://minio:9000", AccessKey: "key", SecretKey: "secret",
		Region: "us-east-1", ForcePathStyle: true,
	}
	m := cfg.ToMap()
	if m["endpoint"] != "http://minio:9000" || m["forcePathStyle"] != "true" {
		t.Errorf("unexpected map: %v", m)
	}
	cfg.ForcePathStyle = false
	if _, ok := cfg.ToMap()["forcePathStyle"]; ok {
		t.Error("forcePathStyle should be absent when false")
	}
}
