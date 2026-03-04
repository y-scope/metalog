package db

import (
	"fmt"
	"regexp"
)

// sqlIdentRe validates SQL identifiers used as table/column names.
// Only lowercase alphanumeric and underscores are allowed.
var sqlIdentRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,63}$`)

// ValidateSQLIdentifier returns an error if name is not a safe SQL identifier.
func ValidateSQLIdentifier(name string) error {
	if !sqlIdentRe.MatchString(name) {
		return fmt.Errorf("invalid SQL identifier: %q", name)
	}
	return nil
}

// QuoteIdentifier wraps a SQL identifier in backticks.
// The identifier MUST be validated first via ValidateSQLIdentifier.
func QuoteIdentifier(name string) string {
	return "`" + name + "`"
}
