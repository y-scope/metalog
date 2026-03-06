package query

import (
	"testing"
)

func TestValidateFilterExpression_ValidExpressions(t *testing.T) {
	validExpressions := []string{
		// Empty
		"",
		// Simple comparisons
		"min_timestamp <= 1679976000",
		"min_timestamp >= 1679976000",
		"max_timestamp < 1679976000",
		"max_timestamp > 1679976000",
		"state = 'IR_CLOSED'",
		"record_count = 100",
		// Boolean logic
		"min_timestamp > 1000 AND max_timestamp < 2000",
		"state = 'IR_CLOSED' OR state = 'IR_PURGING'",
		"(min_timestamp > 1000) AND (state = 'IR_CLOSED')",
		// NOT expressions
		"NOT state = 'IR_PURGING'",
		"state != 'IR_PURGING'",
		"state <> 'IR_PURGING'",
		// IN clauses
		"state IN ('IR_CLOSED', 'IR_PURGING')",
		"id IN (1, 2, 3, 4, 5)",
		// NULL checks
		"clp_ir_path IS NULL",
		"clp_archive_path IS NOT NULL",
		// BETWEEN
		"min_timestamp BETWEEN 1000 AND 2000",
		// Complex nested
		"((min_timestamp > 1000 AND max_timestamp < 2000) OR state = 'IR_CLOSED') AND record_count > 0",
		// LIKE patterns
		"clp_ir_path LIKE '/logs/%'",
		"clp_ir_path NOT LIKE '/tmp/%'",
		"clp_ir_path LIKE '/logs/app_%'",
		// Case variations
		"MIN_TIMESTAMP > 1000",
		"State = 'IR_CLOSED'",
		// Negative numbers
		"record_count > -1",
	}

	for _, expr := range validExpressions {
		if err := ValidateFilterExpression(expr); err != nil {
			t.Errorf("ValidateFilterExpression(%q) = %v, want nil", expr, err)
		}
	}
}

func TestValidateFilterExpression_RejectsSQLInjection(t *testing.T) {
	tests := []string{
		// Multi-statement
		"state = 'IR_CLOSED'; DROP TABLE users",
		// Subqueries
		"id IN (SELECT id FROM users)",
		// Function calls
		"SLEEP(5)",
		"BENCHMARK(10000000, SHA1('x'))",
		"LOAD_FILE('/etc/passwd')",
		// UNION injection
		"1=1 UNION SELECT * FROM users",
		// DDL/DML embedded
		"1=1; DROP TABLE files",
		// Variable assignment
		"@a := 1",
	}

	for _, expr := range tests {
		if err := ValidateFilterExpression(expr); err == nil {
			t.Errorf("ValidateFilterExpression(%q) = nil, want error", expr)
		}
	}
}

func TestValidateFilterExpression_RejectsUnsafeExprTypes(t *testing.T) {
	tests := []string{
		// Subquery in comparison
		"id = (SELECT MAX(id) FROM files)",
		// EXISTS
		"EXISTS (SELECT 1 FROM users)",
		// CASE expression
		"CASE WHEN state = 'x' THEN 1 ELSE 0 END = 1",
	}

	for _, expr := range tests {
		if err := ValidateFilterExpression(expr); err == nil {
			t.Errorf("ValidateFilterExpression(%q) = nil, want error", expr)
		}
	}
}

func TestValidateFilterExpression_ParseErrors(t *testing.T) {
	tests := []string{
		// Malformed SQL
		"AND AND AND",
		"= = =",
		"(((",
		")))",
	}

	for _, expr := range tests {
		if err := ValidateFilterExpression(expr); err == nil {
			t.Errorf("ValidateFilterExpression(%q) = nil, want error", expr)
		}
	}
}

func TestValidateFilterExpression_SafeWordsInValues(t *testing.T) {
	// Words that look dangerous but are safe as string literals or column names
	safe := []string{
		"column_name = 'dropbox'",
		"status = 'updated'",
		"deleted_at IS NOT NULL",
	}

	for _, expr := range safe {
		if err := ValidateFilterExpression(expr); err != nil {
			t.Errorf("ValidateFilterExpression(%q) = %v, want nil", expr, err)
		}
	}
}
