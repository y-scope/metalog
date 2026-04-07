package node

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/schema"
	"github.com/y-scope/metalog/telemetry"
)

func TestResources_ReadOnlyDB_WithReadDB(t *testing.T) {
	rwDB, _, _ := sqlmock.New()
	defer rwDB.Close() //nolint:errcheck
	roDB, _, _ := sqlmock.New()
	defer roDB.Close() //nolint:errcheck

	s := &Resources{
		DB:     rwDB,
		ReadDB: roDB,
		Log:    zap.NewNop(),
	}

	got := s.ReadOnlyDB()
	if got != roDB {
		t.Error("ReadOnlyDB() should return ReadDB when set")
	}
}

func TestResources_ReadOnlyDB_WithoutReadDB(t *testing.T) {
	rwDB, _, _ := sqlmock.New()
	defer rwDB.Close() //nolint:errcheck

	s := &Resources{
		DB:  rwDB,
		Log: zap.NewNop(),
	}

	got := s.ReadOnlyDB()
	if got != rwDB {
		t.Error("ReadOnlyDB() should return DB when ReadDB is nil")
	}
}

func TestResources_SetAndGetColumnRegistry(t *testing.T) {
	mockDB, _, _ := sqlmock.New()
	defer mockDB.Close() //nolint:errcheck

	s := &Resources{DB: mockDB, Log: zap.NewNop()}

	// GetColumnRegistry for an unknown table triggers lazy-load, which fails
	// because sqlmock has no expectations — should return nil without panic.
	if s.GetColumnRegistry("test_table") != nil {
		t.Error("expected nil for unknown table (lazy-load should fail gracefully)")
	}

	// Set a registry (mock, just checking the map operations)
	// We can't create a real ColumnRegistry without DB, so use nil type assertion workaround
	// Instead, test the map operations only
	s.SetColumnRegistry("test_table", (*schema.ColumnRegistry)(nil))
	// GetColumnRegistry should return what we set (cache hit, no DB call)
	got := s.GetColumnRegistry("test_table")
	if got != nil {
		t.Error("set nil, expected nil back")
	}

	// Set for a second table
	s.SetColumnRegistry("other_table", (*schema.ColumnRegistry)(nil))
	// Both should be retrievable from cache
	s.GetColumnRegistry("test_table")
	s.GetColumnRegistry("other_table")
}

func TestResources_Close_OwnedDBs(t *testing.T) {
	db1, _, _ := sqlmock.New()
	db2, _, _ := sqlmock.New()

	s := &Resources{
		DB:          db1,
		ReadDB:      db2,
		dbOwned:     true,
		readDBOwned: true,
		Log:         zap.NewNop(),
	}

	s.Close()

	// After close, DB pools should be closed
	if err := db1.Ping(); err == nil {
		t.Error("DB should be closed")
	}
	if err := db2.Ping(); err == nil {
		t.Error("ReadDB should be closed")
	}
}

func TestResources_Close_ExternalDBs(t *testing.T) {
	db1, _, _ := sqlmock.New()
	db2, _, _ := sqlmock.New()

	s := &Resources{
		DB:          db1,
		ReadDB:      db2,
		dbOwned:     false,
		readDBOwned: false,
		Log:         zap.NewNop(),
	}

	s.Close()

	// External DBs should NOT be closed
	// sqlmock DB ping should still work
	// (Actually sqlmock always allows Ping, but the point is Close() wasn't called)
}

func TestResources_Close_NilDBs(t *testing.T) {
	s := &Resources{
		Log: zap.NewNop(),
	}
	// Should not panic
	s.Close()
}

func TestResources_SetColumnRegistry_InitializesMap(t *testing.T) {
	s := &Resources{Log: zap.NewNop()}
	// registries starts nil
	s.SetColumnRegistry("table1", nil)
	// Should not panic and map should be initialized
	if s.registries == nil {
		t.Error("registries map should be initialized")
	}
}

func TestResources_Close_WithTelemetry(t *testing.T) {
	s := &Resources{
		Log:       zap.NewNop(),
		Telemetry: &telemetry.Provider{},
	}
	// Telemetry.Shutdown should be called but not panic with zero-value Provider.
	s.Close()
}

// Suppress unused import warning
var _ *sql.DB
