package db

import "strings"

// OnDuplicateKeyUpdateValues generates an ON DUPLICATE KEY UPDATE clause
// using MariaDB / MySQL < 8.0.20 VALUES() syntax.
//
//	OnDuplicateKeyUpdateValues("a", "b")
//	=> "ON DUPLICATE KEY UPDATE a = VALUES(a), b = VALUES(b)"
func OnDuplicateKeyUpdateValues(cols ...string) string {
	parts := make([]string, len(cols))
	for i, col := range cols {
		parts[i] = col + " = VALUES(" + col + ")"
	}
	return "ON DUPLICATE KEY UPDATE " + strings.Join(parts, ", ")
}

// OnDuplicateKeyUpdateAlias generates an AS alias + ON DUPLICATE KEY UPDATE
// clause using MySQL 8.0.20+ row alias syntax.
//
//	OnDuplicateKeyUpdateAlias("new", "a", "b")
//	=> "AS new ON DUPLICATE KEY UPDATE a = new.a, b = new.b"
func OnDuplicateKeyUpdateAlias(alias string, cols ...string) string {
	parts := make([]string, len(cols))
	for i, col := range cols {
		parts[i] = col + " = " + alias + "." + col
	}
	return "AS " + alias + " ON DUPLICATE KEY UPDATE " + strings.Join(parts, ", ")
}
