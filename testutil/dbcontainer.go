// Package testutil provides shared test infrastructure for integration tests.
//
// Integration tests use testcontainers-go to start database containers.
// The default image is MariaDB 10.6, but the code is compatible with
// MySQL 8.0+ and most MySQL-compatible derivatives.
//
// In rootless Docker environments (devpods, some CI systems), the Ryuk
// reaper container may fail to start. Set the environment variable
// TESTCONTAINERS_RYUK_DISABLED=true to work around this. See the README
// "Docker troubleshooting" section for details.
package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mariadb"

	"github.com/y-scope/metalog/schema"
)

const (
	dbImage    = "mariadb:10.6"
	dbName     = "metalog_test"
	dbUser     = "root"
	dbPassword = "test"
)

// DBContainer holds a running database container and a connected *sql.DB.
type DBContainer struct {
	Container testcontainers.Container
	DB        *sql.DB
	DSN       string
}

// SetupDB starts a MySQL-compatible testcontainer and returns a connected *sql.DB.
// The caller should defer Teardown().
func SetupDB(t *testing.T) *DBContainer {
	t.Helper()
	ctx := context.Background()

	container, err := mariadb.Run(ctx,
		dbImage,
		mariadb.WithDatabase(dbName),
		mariadb.WithUsername(dbUser),
		mariadb.WithPassword(dbPassword),
	)
	if err != nil {
		t.Fatalf("failed to start database container: %v", err)
	}

	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	connStr, err := container.ConnectionString(ctx, "parseTime=true", "interpolateParams=true")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	return &DBContainer{
		Container: container,
		DB:        db,
		DSN:       connStr,
	}
}

// Teardown closes the database connection and terminates the container.
func (mc *DBContainer) Teardown(t *testing.T) {
	t.Helper()
	if mc.DB != nil {
		_ = mc.DB.Close()
	}
	if mc.Container != nil {
		_ = mc.Container.Terminate(context.Background())
	}
}

// LoadSchema executes the embedded schema DDL against the database.
func (mc *DBContainer) LoadSchema(t *testing.T) {
	t.Helper()
	for _, stmt := range splitStatements(schema.SchemaSQL) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := mc.DB.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("schema statement failed:\n%s\nerror: %v", truncate(stmt, 200), err)
		}
	}
}

// CreateTestTable provisions a metadata table by cloning _clp_template
// and inserting the required registry rows.
func (mc *DBContainer) CreateTestTable(t *testing.T, tableName string) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{
		fmt.Sprintf("INSERT IGNORE INTO _table (table_name, display_name) VALUES ('%s', '%s')", tableName, tableName),
		fmt.Sprintf("INSERT IGNORE INTO _table_config (table_name) VALUES ('%s')", tableName),
		fmt.Sprintf("INSERT IGNORE INTO _table_assignment (table_name) VALUES ('%s')", tableName),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` LIKE _clp_template", tableName),
	} {
		if _, err := mc.DB.ExecContext(ctx, q); err != nil {
			t.Fatalf("CreateTestTable(%s): %v", tableName, err)
		}
	}
}

func splitStatements(sql string) []string {
	lines := strings.Split(sql, "\n")
	var filtered []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		filtered = append(filtered, line)
	}
	content := strings.Join(filtered, "\n")
	return strings.Split(content, ";")
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
