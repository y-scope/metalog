package db

import (
	"fmt"
	"regexp"
)

// sqlIdentRe validates SQL identifiers: lowercase letters, digits, and underscores,
// starting with a letter or underscore, 1–64 characters.
var sqlIdentRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,63}$`)

// ValidateSQLIdentifier returns an error if name is not a safe SQL identifier.
func ValidateSQLIdentifier(name string) error {
	if !sqlIdentRe.MatchString(name) {
		return fmt.Errorf("invalid SQL identifier: %q", name)
	}
	return nil
}

// QuoteIdentifier wraps a SQL identifier in backticks.
// Panics if name fails [ValidateSQLIdentifier] — this is a programming error,
// not a runtime condition.
func QuoteIdentifier(name string) string {
	if err := ValidateSQLIdentifier(name); err != nil {
		panic("db.QuoteIdentifier: " + err.Error())
	}
	return "`" + name + "`"
}
