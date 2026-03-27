package config

import (
	"testing"
)

func TestApplyEnvOverrides_MultipleFields(t *testing.T) {
	cfg := &NodeConfig{
		Database: DatabaseSection{
			Primary: DatabaseConfig{Host: "yaml-host", Port: 3306},
		},
	}
	t.Setenv("DB_PRIMARY_HOST", "env-host")
	t.Setenv("DB_PRIMARY_PASSWORD", "secret")
	t.Setenv("GRPC_PORT", "50051")
	t.Setenv("COORDINATOR_NAME", "node-1")

	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Primary.Host != "env-host" {
		t.Errorf("Host = %q, want env-host", cfg.Database.Primary.Host)
	}
	if cfg.Database.Primary.Password != "secret" {
		t.Errorf("Password = %q, want secret", cfg.Database.Primary.Password)
	}
	if cfg.GRPC.Port != 50051 {
		t.Errorf("GRPC.Port = %d, want 50051", cfg.GRPC.Port)
	}
	// Port should remain unchanged (no env override set for it).
	if cfg.Database.Primary.Port != 3306 {
		t.Errorf("Port = %d, want 3306", cfg.Database.Primary.Port)
	}
}

func TestApplyEnvOverrides_InvalidInt(t *testing.T) {
	cfg := &NodeConfig{}
	t.Setenv("GRPC_PORT", "abc")
	if err := ApplyEnvOverrides(cfg); err == nil {
		t.Fatal("expected error for invalid int")
	}
}

func TestApplyEnvOverrides_ReplicaPointer(t *testing.T) {
	cfg := &NodeConfig{
		Database: DatabaseSection{Replica: &DatabaseConfig{Host: "original"}},
	}
	t.Setenv("DB_REPLICA_HOST", "override")
	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Replica.Host != "override" {
		t.Errorf("Replica.Host = %q, want override", cfg.Database.Replica.Host)
	}
}

func TestApplyEnvOverrides_BoolField(t *testing.T) {
	cfg := &NodeConfig{}
	t.Setenv("HEALTH_ENABLED", "true")
	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.Health.Enabled {
		t.Error("Health.Enabled should be true")
	}
}

func TestApplyEnvOverrides_InvalidBool(t *testing.T) {
	cfg := &NodeConfig{}
	t.Setenv("HEALTH_ENABLED", "not-a-bool")
	if err := ApplyEnvOverrides(cfg); err == nil {
		t.Fatal("expected error for invalid bool")
	}
}

func TestApplyEnvOverrides_NilPointerAndEmptyEnv(t *testing.T) {
	cfg := &NodeConfig{
		Database: DatabaseSection{Primary: DatabaseConfig{Host: "keep"}},
	}
	// Replica is nil — should not panic.
	// Empty env var — should not override.
	t.Setenv("DB_PRIMARY_HOST", "")
	if err := ApplyEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Primary.Host != "keep" {
		t.Errorf("Host = %q, want keep (empty env should not override)", cfg.Database.Primary.Host)
	}
}
