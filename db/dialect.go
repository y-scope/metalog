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
)

func (d DatabaseType) String() string {
	switch d {
	case DatabaseTypeMySQL:
		return "MySQL"
	case DatabaseTypeMariaDB:
		return "MariaDB"
	case DatabaseTypeAurora:
		return "Aurora"
	default:
		return "Unknown"
	}
}

// DetectDatabaseType queries the server version string to determine the engine.
//
// Detection relies on markers in the VERSION() output:
//   - MariaDB — version contains "mariadb" (e.g., "10.6.12-MariaDB")
//   - Aurora — version contains "aurora" (e.g., "5.7.mysql_aurora.2.10.2")
//   - Everything else — reported as MySQL. This includes vanilla MySQL,
//     GCP Cloud SQL, Azure Database for MySQL, Percona, and TiDB, which all
//     return standard MySQL version strings with no distinguishing marker.
func DetectDatabaseType(ctx context.Context, db *sql.DB) (DatabaseType, string, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return DatabaseTypeUnknown, "", err
	}

	lower := strings.ToLower(version)
	switch {
	case strings.Contains(lower, "mariadb"):
		return DatabaseTypeMariaDB, version, nil
	case strings.Contains(lower, "aurora"):
		return DatabaseTypeAurora, version, nil
	default:
		return DatabaseTypeMySQL, version, nil
	}
}
