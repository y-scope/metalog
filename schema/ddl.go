package schema

// lockMode returns the appropriate LOCK clause for online DDL.
// MariaDB requires LOCK=SHARED for some in-place operations;
// MySQL 8.0+ supports LOCK=NONE.
func lockMode(isMariaDB bool) string {
	if isMariaDB {
		return "SHARED"
	}
	return "NONE"
}
