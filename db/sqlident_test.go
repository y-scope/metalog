package db

import "testing"

func TestValidateSQLIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		// Valid identifiers
		{"my_table", false},
		{"a", false},
		{"_private", false},
		{"dim_f01", false},
		{"agg_f99", false},
		{"a123", false},
		{"_clp_template", false},

		// Invalid identifiers
		{"", true},
		{"1starts_with_digit", true},
		{"UpperCase", true},
		{"has-dash", true},
		{"has space", true},
		{"has.dot", true},
		{"has;semicolon", true},
		{"a`backtick", true},
		{"SELECT", true},         // uppercase
		{"drop_table; --", true}, // injection attempt

		// Length boundary: 64 chars is max (1 prefix + 63 = 64 total)
		{"a_23456789_123456789_123456789_123456789_123456789_1234567890_23", false}, // exactly 64
		{"a_23456789_123456789_123456789_123456789_123456789_1234567890_234", true}, // 65 chars
	}
	for _, tt := range tests {
		err := ValidateSQLIdentifier(tt.name)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateSQLIdentifier(%q) error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}
