//go:build integration

package schema_test

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/schema"
	"github.com/y-scope/metalog/internal/testutil"
)

func TestBaseSchemaValidator_Validate(t *testing.T) {
	mc := testutil.SetupMariaDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, "test_validator")

	log := zap.NewNop()
	v := schema.NewBaseSchemaValidator(mc.DB, log)
	if err := v.Validate(context.Background()); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestBaseSchemaValidator_MissingColumn(t *testing.T) {
	mc := testutil.SetupMariaDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)

	ctx := context.Background()
	// Drop a column from the template to simulate a stale schema.
	_, err := mc.DB.ExecContext(ctx, "ALTER TABLE _clp_template DROP COLUMN ext")
	if err != nil {
		t.Fatalf("drop column: %v", err)
	}

	log := zap.NewNop()
	v := schema.NewBaseSchemaValidator(mc.DB, log)
	err = v.Validate(ctx)
	if err == nil {
		t.Fatal("expected error for missing column, got nil")
	}
	if got := err.Error(); !contains(got, "ext") {
		t.Errorf("error should mention missing column 'ext', got: %s", got)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
