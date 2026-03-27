package metastore

import (
	"os"
	"testing"
)

func TestParseRequiredEnv_Empty(t *testing.T) {
	result := ParseRequiredEnv("")
	if result != nil {
		t.Errorf("got %v, want nil for empty string", result)
	}
}

func TestParseRequiredEnv_Single(t *testing.T) {
	result := ParseRequiredEnv("REGION=us-east")
	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	if result["REGION"] != "us-east" {
		t.Errorf("REGION = %q, want us-east", result["REGION"])
	}
}

func TestParseRequiredEnv_Multiple(t *testing.T) {
	result := ParseRequiredEnv("REGION=us-east,CLUSTER=prod")
	if len(result) != 2 {
		t.Fatalf("len = %d, want 2", len(result))
	}
	if result["REGION"] != "us-east" {
		t.Errorf("REGION = %q, want us-east", result["REGION"])
	}
	if result["CLUSTER"] != "prod" {
		t.Errorf("CLUSTER = %q, want prod", result["CLUSTER"])
	}
}

func TestParseRequiredEnv_ValueWithEquals(t *testing.T) {
	result := ParseRequiredEnv("KEY=val=ue")
	if result["KEY"] != "val=ue" {
		t.Errorf("KEY = %q, want val=ue", result["KEY"])
	}
}

func TestMatchesEnv_EmptyMatchesAny(t *testing.T) {
	if !MatchesEnv("") {
		t.Error("empty required_env should match any node")
	}
}

func TestMatchesEnv_SingleMatch(t *testing.T) {
	os.Setenv("TEST_REGION", "us-east")
	defer os.Unsetenv("TEST_REGION")

	if !MatchesEnv("TEST_REGION=us-east") {
		t.Error("should match")
	}
}

func TestMatchesEnv_SingleMismatch(t *testing.T) {
	os.Setenv("TEST_REGION", "eu-west")
	defer os.Unsetenv("TEST_REGION")

	if MatchesEnv("TEST_REGION=us-east") {
		t.Error("should not match")
	}
}

func TestMatchesEnv_MultipleAllMatch(t *testing.T) {
	os.Setenv("TEST_REGION", "us-east")
	os.Setenv("TEST_CLUSTER", "prod")
	defer os.Unsetenv("TEST_REGION")
	defer os.Unsetenv("TEST_CLUSTER")

	if !MatchesEnv("TEST_REGION=us-east,TEST_CLUSTER=prod") {
		t.Error("should match when all conditions met")
	}
}

func TestMatchesEnv_MultiplePartialMismatch(t *testing.T) {
	os.Setenv("TEST_REGION", "us-east")
	os.Setenv("TEST_CLUSTER", "staging")
	defer os.Unsetenv("TEST_REGION")
	defer os.Unsetenv("TEST_CLUSTER")

	if MatchesEnv("TEST_REGION=us-east,TEST_CLUSTER=prod") {
		t.Error("should not match when one condition fails (AND semantics)")
	}
}

func TestValidateRequiredEnv_Valid(t *testing.T) {
	if err := ValidateRequiredEnv("REGION=us-east,CLUSTER=prod"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateRequiredEnv_Empty(t *testing.T) {
	if err := ValidateRequiredEnv(""); err != nil {
		t.Errorf("empty should be valid: %v", err)
	}
}

func TestValidateRequiredEnv_MalformedNoPairs(t *testing.T) {
	err := ValidateRequiredEnv("REGION_us-east")
	if err == nil {
		t.Error("should reject required_env with no valid KEY=VALUE pairs")
	}
}

func TestMatchesEnv_MissingEnvVar(t *testing.T) {
	os.Unsetenv("TEST_NONEXISTENT")

	if MatchesEnv("TEST_NONEXISTENT=anything") {
		t.Error("should not match when env var is not set")
	}
}
