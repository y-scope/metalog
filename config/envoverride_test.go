package config

import (
	"testing"
)

func TestApplyEnvOverrides_StringField(t *testing.T) {
	cfg := &NodeConfig{}
	t.Setenv("COORDINATOR_NAME", "my-node")

	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Coordinator.Name != "my-node" {
		t.Errorf("Coordinator.Name = %q, want %q", cfg.Coordinator.Name, "my-node")
	}
}

func TestApplyEnvOverrides_IntField(t *testing.T) {
	cfg := &NodeConfig{}
	t.Setenv("GRPC_PORT", "8080")

	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.GRPC.Port != 8080 {
		t.Errorf("GRPC.Port = %d, want 8080", cfg.GRPC.Port)
	}
}

func TestApplyEnvOverrides_InvalidInt(t *testing.T) {
	cfg := &NodeConfig{}
	t.Setenv("GRPC_PORT", "abc")

	err := ApplyEnvOverrides(cfg)
	if err == nil {
		t.Fatal("expected error for invalid int, got nil")
	}
}

func TestApplyEnvOverrides_Prefix(t *testing.T) {
	cfg := &NodeConfig{}
	t.Setenv("DB_PRIMARY_HOST", "override-host")
	t.Setenv("DB_PRIMARY_PORT", "3307")

	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Primary.Host != "override-host" {
		t.Errorf("Database.Primary.Host = %q, want %q", cfg.Database.Primary.Host, "override-host")
	}
	if cfg.Database.Primary.Port != 3307 {
		t.Errorf("Database.Primary.Port = %d, want 3307", cfg.Database.Primary.Port)
	}
}

func TestApplyEnvOverrides_ReplicaPrefix(t *testing.T) {
	cfg := &NodeConfig{
		Database: DatabaseSection{
			Replica: &DatabaseConfig{Host: "original"},
		},
	}
	t.Setenv("DB_REPLICA_HOST", "replica-override")

	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Replica.Host != "replica-override" {
		t.Errorf("Database.Replica.Host = %q, want %q", cfg.Database.Replica.Host, "replica-override")
	}
}

func TestApplyEnvOverrides_NilPointer(t *testing.T) {
	cfg := &NodeConfig{} // Replica is nil

	// Should not panic.
	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
}

func TestApplyEnvOverrides_EmptyEnvVar(t *testing.T) {
	cfg := &NodeConfig{
		Database: DatabaseSection{
			Primary: DatabaseConfig{Host: "original"},
		},
	}
	t.Setenv("DB_PRIMARY_HOST", "")

	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Primary.Host != "original" {
		t.Errorf("Database.Primary.Host = %q, want %q (empty env should not override)", cfg.Database.Primary.Host, "original")
	}
}

func TestApplyEnvOverrides_UnsetEnvVar(t *testing.T) {
	cfg := &NodeConfig{
		Database: DatabaseSection{
			Primary: DatabaseConfig{Host: "original"},
		},
	}
	// DB_PRIMARY_HOST is not set.

	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Primary.Host != "original" {
		t.Errorf("Database.Primary.Host = %q, want %q", cfg.Database.Primary.Host, "original")
	}
}

func TestApplyEnvOverrides_MultipleFields(t *testing.T) {
	cfg := &NodeConfig{}
	t.Setenv("DB_PRIMARY_HOST", "db.example.com")
	t.Setenv("DB_PRIMARY_PASSWORD", "secret")
	t.Setenv("GRPC_PORT", "50051")
	t.Setenv("HEALTH_PORT", "9999")
	t.Setenv("COORDINATOR_NAME", "node-1")

	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Primary.Host != "db.example.com" {
		t.Errorf("Host = %q", cfg.Database.Primary.Host)
	}
	if cfg.Database.Primary.Password != "secret" {
		t.Errorf("Password = %q", cfg.Database.Primary.Password)
	}
	if cfg.GRPC.Port != 50051 {
		t.Errorf("GRPC.Port = %d", cfg.GRPC.Port)
	}
	if cfg.Health.Port != 9999 {
		t.Errorf("Health.Port = %d", cfg.Health.Port)
	}
	if cfg.Coordinator.Name != "node-1" {
		t.Errorf("Coordinator.Name = %q", cfg.Coordinator.Name)
	}
}

func TestApplyEnvOverrides_YAMLThenEnv(t *testing.T) {
	// Simulate: YAML sets host to "yaml-host", env overrides to "env-host".
	cfg := &NodeConfig{
		Database: DatabaseSection{
			Primary: DatabaseConfig{Host: "yaml-host", Port: 3306},
		},
	}
	t.Setenv("DB_PRIMARY_HOST", "env-host")

	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Primary.Host != "env-host" {
		t.Errorf("Host = %q, want env-host", cfg.Database.Primary.Host)
	}
	// Port should remain unchanged.
	if cfg.Database.Primary.Port != 3306 {
		t.Errorf("Port = %d, want 3306", cfg.Database.Primary.Port)
	}
}
