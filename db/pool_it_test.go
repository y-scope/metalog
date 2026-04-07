package db_test

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/db"
	"github.com/y-scope/metalog/testutil"
)

// dbConfigFromDSN builds a DatabaseConfig by parsing a go-sql-driver DSN string.
func dbConfigFromDSN(t *testing.T, dsn string) config.DatabaseConfig {
	t.Helper()
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse DSN %q: %v", dsn, err)
	}
	host, portStr, err := net.SplitHostPort(parsed.Addr)
	if err != nil {
		t.Fatalf("split host/port from %q: %v", parsed.Addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port %q: %v", portStr, err)
	}
	return config.DatabaseConfig{
		Host:     host,
		Port:     port,
		Database: parsed.DBName,
		User:     parsed.User,
		Password: parsed.Passwd,
	}
}

// TestNewPool_HappyPath_DefaultPoolSettings verifies that NewPool connects
// successfully and applies the default pool size (5) and min-idle (2) when
// PoolSize and PoolMinIdle are left at zero.
func TestNewPool_HappyPath_DefaultPoolSettings(t *testing.T) {
	mc := testutil.SetupDB(t)
	defer mc.Teardown(t)

	cfg := dbConfigFromDSN(t, mc.DSN)
	// PoolSize and PoolMinIdle are zero → defaults (5 / 2) should be applied.

	pool, err := db.NewPool(cfg)
	if err != nil {
		t.Fatalf("NewPool with default settings: %v", err)
	}
	defer pool.Close() //nolint:errcheck // test cleanup

	stats := pool.Stats()
	if stats.MaxOpenConnections != 5 {
		t.Errorf("MaxOpenConnections = %d, want 5", stats.MaxOpenConnections)
	}
}

// TestNewPool_HappyPath_CustomPoolSettings verifies that explicitly provided
// PoolSize and PoolMinIdle values are honoured.
func TestNewPool_HappyPath_CustomPoolSettings(t *testing.T) {
	mc := testutil.SetupDB(t)
	defer mc.Teardown(t)

	cfg := dbConfigFromDSN(t, mc.DSN)
	cfg.PoolSize = 20
	cfg.PoolMinIdle = 5

	pool, err := db.NewPool(cfg)
	if err != nil {
		t.Fatalf("NewPool with custom pool settings: %v", err)
	}
	defer pool.Close() //nolint:errcheck // test cleanup

	stats := pool.Stats()
	if stats.MaxOpenConnections != 20 {
		t.Errorf("MaxOpenConnections = %d, want 20", stats.MaxOpenConnections)
	}
}

// TestNewPool_BadDSN verifies that NewPool returns an error when the target
// database is unreachable (bad host/port so Ping fails).
func TestNewPool_BadDSN(t *testing.T) {
	cfg := config.DatabaseConfig{
		Host:     "127.0.0.1",
		Port:     1, // nothing listening on port 1
		Database: "nonexistent",
		User:     "root",
		Password: "wrong",
	}

	pool, err := db.NewPool(cfg)
	if err == nil {
		pool.Close() //nolint:errcheck // test cleanup
		t.Fatal("NewPool with bad DSN: expected error, got nil")
	}
	if !strings.HasPrefix(err.Error(), "ping database:") {
		t.Errorf("error %q does not start with %q", err.Error(), "ping database:")
	}
}
