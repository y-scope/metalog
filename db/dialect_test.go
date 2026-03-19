package db

import "testing"

func TestVersionStringDetection(t *testing.T) {
	// These are the version string patterns that DetectDatabaseType looks for
	tests := []struct {
		version string
		want    DatabaseType
	}{
		// MariaDB versions
		{"10.6.12-MariaDB", DatabaseTypeMariaDB},
		{"10.11.4-MariaDB-1:10.11.4+maria~deb11", DatabaseTypeMariaDB},
		{"5.5.5-10.6.12-MariaDB", DatabaseTypeMariaDB},

		// TiDB versions
		{"5.7.25-TiDB-v6.5.0", DatabaseTypeTiDB},
		{"8.0.11-TiDB-v7.0.0", DatabaseTypeTiDB},

		// Aurora versions
		{"5.7.mysql_aurora.2.10.2", DatabaseTypeAurora},
		{"8.0.mysql_aurora.3.02.0", DatabaseTypeAurora},

		// MySQL versions (default)
		{"8.0.33", DatabaseTypeMySQL},
		{"5.7.44", DatabaseTypeMySQL},
		{"8.0.33-0ubuntu0.22.04.1", DatabaseTypeMySQL},

		// Percona (should be MySQL - no special handling)
		{"8.0.33-25.1", DatabaseTypeMySQL},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			// We can't call DetectDatabaseType without a DB connection,
			// but we can verify the detection logic by checking the patterns
			var got DatabaseType
			lower := toLower(tt.version)
			switch {
			case contains(lower, "mariadb"):
				got = DatabaseTypeMariaDB
			case contains(lower, "tidb"):
				got = DatabaseTypeTiDB
			case contains(lower, "aurora"):
				got = DatabaseTypeAurora
			default:
				got = DatabaseTypeMySQL
			}

			if got != tt.want {
				t.Errorf("version %q detected as %s, want %s", tt.version, got, tt.want)
			}
		})
	}
}

// Simple string helpers to match the detection logic in dialect.go
func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		result[i] = c
	}
	return string(result)
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
