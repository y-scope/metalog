// Package testutil provides shared test infrastructure for integration tests.
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

	schemadef "github.com/y-scope/metalog/schema"
)

const (
	mariadbImage = "mariadb:10.6"
	dbName       = "metalog_test"
	dbUser       = "root"
	dbPassword   = "test"
)

// MariaDBContainer holds a running MariaDB container and a connected *sql.DB.
type MariaDBContainer struct {
	Container testcontainers.Container
	DB        *sql.DB
	DSN       string
}

// SetupMariaDB starts a MariaDB testcontainer and returns a connected *sql.DB.
// The caller should defer Teardown().
func SetupMariaDB(t *testing.T) *MariaDBContainer {
	t.Helper()
	ctx := context.Background()

	container, err := mariadb.Run(ctx,
		mariadbImage,
		mariadb.WithDatabase(dbName),
		mariadb.WithUsername(dbUser),
		mariadb.WithPassword(dbPassword),
	)
	if err != nil {
		t.Fatalf("failed to start MariaDB container: %v", err)
	}

	connStr, err := container.ConnectionString(ctx, "parseTime=true", "interpolateParams=true")
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		container.Terminate(ctx)
		t.Fatalf("failed to ping database: %v", err)
	}

	return &MariaDBContainer{
		Container: container,
		DB:        db,
		DSN:       connStr,
	}
}

// Teardown closes the database connection and terminates the container.
func (mc *MariaDBContainer) Teardown(t *testing.T) {
	t.Helper()
	if mc.DB != nil {
		mc.DB.Close()
	}
	if mc.Container != nil {
		mc.Container.Terminate(context.Background())
	}
}

// LoadSchema reads schema.sql and executes all statements against the database.
func (mc *MariaDBContainer) LoadSchema(t *testing.T) {
	t.Helper()

	schemaSQL := schemadef.SQL

	// Execute each statement separately
	stmts := splitStatements(schemaSQL)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := mc.DB.ExecContext(context.Background(), stmt); err != nil {
			// Ignore ALTER TABLE ... IF EXISTS errors (schema migrations for existing installations)
			if strings.Contains(stmt, "IF EXISTS") || strings.Contains(stmt, "IF NOT EXISTS") {
				continue
			}
			t.Fatalf("failed to execute schema statement:\n%s\nerror: %v", truncate(stmt, 200), err)
		}
	}
}

// CreateTestTable provisions a metadata table by cloning _clp_template
// and inserting the required registry rows.
func (mc *MariaDBContainer) CreateTestTable(t *testing.T, tableName string) {
	t.Helper()
	ctx := context.Background()

	// Insert into _table registry
	_, err := mc.DB.ExecContext(ctx,
		"INSERT IGNORE INTO _table (table_name, display_name) VALUES (?, ?)",
		tableName, tableName)
	if err != nil {
		t.Fatalf("insert _table: %v", err)
	}

	// Insert _table_config
	_, err = mc.DB.ExecContext(ctx,
		"INSERT IGNORE INTO _table_config (table_name) VALUES (?)", tableName)
	if err != nil {
		t.Fatalf("insert _table_config: %v", err)
	}

	// Insert _table_assignment
	_, err = mc.DB.ExecContext(ctx,
		"INSERT IGNORE INTO _table_assignment (table_name) VALUES (?)", tableName)
	if err != nil {
		t.Fatalf("insert _table_assignment: %v", err)
	}

	// Clone _clp_template as the test table
	_, err = mc.DB.ExecContext(ctx,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` LIKE _clp_template", tableName))
	if err != nil {
		t.Fatalf("create table %s: %v", tableName, err)
	}

	// Insert _table_kafka
	_, err = mc.DB.ExecContext(ctx,
		"INSERT IGNORE INTO _table_kafka (table_name, kafka_topic) VALUES (?, ?)",
		tableName, tableName+"-topic")
	if err != nil {
		t.Fatalf("insert _table_kafka: %v", err)
	}
}

func splitStatements(sql string) []string {
	// Strip single-line SQL comments
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
