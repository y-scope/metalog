package db

import (
	"context"
	"database/sql"
	"strings"
)

// DatabaseType identifies the database engine.
type DatabaseType int

const (
	DatabaseTypeUnknown DatabaseType = iota
	DatabaseTypeMySQL
	DatabaseTypeMariaDB
	DatabaseTypeAurora
	DatabaseTypeTiDB
)

func (d DatabaseType) String() string {
	switch d {
	case DatabaseTypeMySQL:
		return "MySQL"
	case DatabaseTypeMariaDB:
		return "MariaDB"
	case DatabaseTypeAurora:
		return "Aurora"
	case DatabaseTypeTiDB:
		return "TiDB"
	default:
		return "Unknown"
	}
}

// DetectDatabaseType queries the server version string to determine the engine.
func DetectDatabaseType(ctx context.Context, db *sql.DB) (DatabaseType, string, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return DatabaseTypeUnknown, "", err
	}

	lower := strings.ToLower(version)
	switch {
	case strings.Contains(lower, "mariadb"):
		return DatabaseTypeMariaDB, version, nil
	case strings.Contains(lower, "tidb"):
		return DatabaseTypeTiDB, version, nil
	case strings.Contains(lower, "aurora"):
		return DatabaseTypeAurora, version, nil
	default:
		return DatabaseTypeMySQL, version, nil
	}
}
