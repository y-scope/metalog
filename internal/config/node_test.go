package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadNodeConfig(t *testing.T) {
	yaml := `
database:
  primary:
    host: localhost
    port: 3306
    database: metalog_metastore
    user: root
    password: password
    poolSize: 5
    poolMinIdle: 2
storage:
  defaultBackend: minio
  backends:
    minio:
      endpoint: http://minio:9000
      bucket: logs
      accessKey: minioadmin
      secretKey: minioadmin
      forcePathStyle: true
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
  clpBinaryPath: /usr/bin/clp-s
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
	if cfg.Database.Primary.Host != "localhost" {
		t.Errorf("Database.Primary.Host = %q, want %q", cfg.Database.Primary.Host, "localhost")
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

func TestLoadNodeConfig_WithReplica(t *testing.T) {
	yaml := `
database:
  primary:
    host: primary-db
    port: 3306
    database: metalog_metastore
    user: root
    password: password
  replica:
    host: replica-db
    port: 3307
    database: metalog_metastore
    user: reader
    password: secret
    poolSize: 10
storage:
  defaultBackend: minio
  backends:
    minio:
      endpoint: http://minio:9000
      accessKey: minioadmin
      secretKey: minioadmin
      forcePathStyle: true
coordinator:
  nodeIdEnvVar: HOSTNAME
tables:
  - name: clp_spark
    kafka:
      topic: spark-ir
      bootstrapServers: kafka:29092
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

	if cfg.Database.Replica == nil {
		t.Fatal("Database.Replica should not be nil")
	}
	if cfg.Database.Replica.Host != "replica-db" {
		t.Errorf("Database.Replica.Host = %q, want %q", cfg.Database.Replica.Host, "replica-db")
	}
	if cfg.Database.Replica.Port != 3307 {
		t.Errorf("Database.Replica.Port = %d, want 3307", cfg.Database.Replica.Port)
	}
	if cfg.Database.Replica.PoolSize != 10 {
		t.Errorf("Database.Replica.PoolSize = %d, want 10", cfg.Database.Replica.PoolSize)
	}

	// EffectiveReplica should return the replica config
	eff := cfg.EffectiveReplica()
	if eff.Host != "replica-db" {
		t.Errorf("EffectiveReplica().Host = %q, want %q", eff.Host, "replica-db")
	}
}

func TestLoadNodeConfig_ReplicaOnly(t *testing.T) {
	yaml := `
database:
  replica:
    host: replica-db
    port: 3306
    database: metalog_metastore
    user: reader
    password: secret
storage:
  defaultBackend: minio
  backends:
    minio:
      endpoint: http://minio:9000
      accessKey: minioadmin
      secretKey: minioadmin
      forcePathStyle: true
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

	// No primary configured
	if cfg.Database.Primary.Host != "" {
		t.Errorf("Database.Primary.Host = %q, want empty", cfg.Database.Primary.Host)
	}

	// EffectiveReplica should return the replica config
	eff := cfg.EffectiveReplica()
	if eff.Host != "replica-db" {
		t.Errorf("EffectiveReplica().Host = %q, want %q", eff.Host, "replica-db")
	}
	if cfg.Database.Replica.Port != 3306 {
		t.Errorf("Database.Replica.Port = %d, want 3306", cfg.Database.Replica.Port)
	}
}

func TestLoadNodeConfig_ReplicaOnlyRejectsCoordinator(t *testing.T) {
	yaml := `
database:
  replica:
    host: replica-db
    port: 3306
    database: metalog_metastore
    user: reader
    password: secret
storage:
  defaultBackend: minio
  backends:
    minio:
      endpoint: http://minio:9000
      accessKey: minioadmin
      secretKey: minioadmin
      forcePathStyle: true
coordinator:
  nodeIdEnvVar: HOSTNAME
tables:
  - name: clp_spark
    kafka:
      topic: spark-ir
      bootstrapServers: kafka:29092
`
	dir := t.TempDir()
	path := filepath.Join(dir, "node.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadNodeConfig(path)
	if err == nil {
		t.Fatal("expected error when coordinator is enabled without primary database")
	}
}

func TestLoadNodeConfig_GRPCQueryOnlyWithReplica(t *testing.T) {
	yaml := `
database:
  replica:
    host: replica-db
    port: 3306
    database: metalog_metastore
    user: reader
    password: secret
storage:
  defaultBackend: minio
  backends:
    minio:
      endpoint: http://minio:9000
      bucket: logs
      accessKey: minioadmin
      secretKey: minioadmin
      forcePathStyle: true
grpc:
  port: 9090
  query: true
  metadata: true
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

	if !cfg.GRPC.Query {
		t.Error("GRPC.Query should be true")
	}
	if !cfg.GRPC.Metadata {
		t.Error("GRPC.Metadata should be true")
	}
	if cfg.HasCoordinator() {
		t.Error("HasCoordinator() should be false")
	}
}

func TestLoadNodeConfig_IngestionWithoutCoordinator(t *testing.T) {
	yaml := `
database:
  primary:
    host: localhost
    port: 3306
    database: metalog_metastore
    user: root
    password: password
storage:
  defaultBackend: minio
  backends:
    minio:
      endpoint: http://minio:9000
      bucket: logs
      accessKey: minioadmin
      secretKey: minioadmin
      forcePathStyle: true
grpc:
  ingestion: true
`
	dir := t.TempDir()
	path := filepath.Join(dir, "node.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadNodeConfig(path)
	if err == nil {
		t.Fatal("expected error when grpc.ingestion is enabled without coordinator")
	}
}

func TestLoadNodeConfig_InvalidDefaultBackend(t *testing.T) {
	yaml := `
database:
  primary:
    host: localhost
    port: 3306
    database: metalog_metastore
    user: root
    password: password
storage:
  defaultBackend: nonexistent
  backends:
    minio:
      endpoint: http://minio:9000
      bucket: logs
      accessKey: minioadmin
      secretKey: minioadmin
      forcePathStyle: true
coordinator:
  nodeIdEnvVar: HOSTNAME
tables:
  - name: clp_spark
    kafka:
      topic: spark-ir
      bootstrapServers: kafka:29092
`
	dir := t.TempDir()
	path := filepath.Join(dir, "node.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadNodeConfig(path)
	if err == nil {
		t.Fatal("expected error when defaultBackend does not match any backend")
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
