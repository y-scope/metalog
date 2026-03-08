package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadNodeConfig(t *testing.T) {
	yaml := `
database:
  host: localhost
  port: 3306
  database: metalog_metastore
  user: root
  password: password
  poolSize: 5
  poolMinIdle: 2
storage:
  irBucket: logs
  archiveBucket: logs
  clpBinaryPath: /usr/bin/clp-s
  defaultBackend: minio
  backends:
    minio:
      endpoint: http://minio:9000
      accessKey: minioadmin
      secretKey: minioadmin
      forcePathStyle: true
server:
  health:
    enabled: true
    port: 8081
coordinator:
  name: test-node
  nodeIdEnvVar: HOSTNAME
  haStrategy: heartbeat
  heartbeatIntervalSeconds: 30
  deadNodeThresholdSeconds: 180
tables:
  - name: clp_spark
    displayName: Spark Logs
    kafka:
      topic: spark-ir
      bootstrapServers: kafka:29092
worker:
  concurrency: 4
`
	dir := t.TempDir()
	path := filepath.Join(dir, "node.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadNodeConfig(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Coordinator.Name != "test-node" {
		t.Errorf("Name = %q, want %q", cfg.Coordinator.Name, "test-node")
	}
	if cfg.Database.Host != "localhost" {
		t.Errorf("Database.Host = %q, want %q", cfg.Database.Host, "localhost")
	}
	if len(cfg.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(cfg.Tables))
	}
	if cfg.Tables[0].Name != "clp_spark" {
		t.Errorf("Tables[0].Name = %q, want %q", cfg.Tables[0].Name, "clp_spark")
	}
	if cfg.Tables[0].Kafka.Topic != "spark-ir" {
		t.Errorf("Tables[0].Kafka.Topic = %q, want %q", cfg.Tables[0].Kafka.Topic, "spark-ir")
	}
}

func TestDSN(t *testing.T) {
	cfg := DatabaseConfig{
		Host:     "localhost",
		Port:     3306,
		Database: "metalog_metastore",
		User:     "root",
		Password: "password",
	}
	got := cfg.DSN()
	want := "root:password@tcp(localhost:3306)/metalog_metastore?interpolateParams=true&parseTime=true&maxAllowedPacket=16777216"
	if got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}
