package metastore

import (
	"testing"
)

func TestParseRequiredEnv(t *testing.T) {
	for _, tt := range []struct {
		want  map[string]string
		name  string
		input string
	}{
		{name: "empty", input: "", want: nil},
		{name: "single", input: "REGION=us-east", want: map[string]string{"REGION": "us-east"}},
		{name: "multiple", input: "REGION=us-east,CLUSTER=prod", want: map[string]string{"REGION": "us-east", "CLUSTER": "prod"}},
		{name: "value with equals", input: "KEY=val=ue", want: map[string]string{"KEY": "val=ue"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseRequiredEnv(tt.input)
			if tt.want == nil {
				if got != nil {
					t.Errorf("got %v, want nil", got)
				}
				return
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("%s = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestMatchesEnv(t *testing.T) {
	for _, tt := range []struct {
		name string
		env  map[string]string
		req  string
		want bool
	}{
		{"empty matches all", nil, "", true},
		{"match", map[string]string{"TEST_REGION": "us-east"}, "TEST_REGION=us-east", true},
		{"mismatch", map[string]string{"TEST_REGION": "eu-west"}, "TEST_REGION=us-east", false},
		{"missing var", nil, "TEST_NONEXISTENT=anything", false},
		{"partial mismatch", map[string]string{"TEST_REGION": "us-east", "TEST_CLUSTER": "staging"}, "TEST_REGION=us-east,TEST_CLUSTER=prod", false},
		{"all match", map[string]string{"TEST_REGION": "us-east", "TEST_CLUSTER": "prod"}, "TEST_REGION=us-east,TEST_CLUSTER=prod", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			if got := MatchesEnv(tt.req); got != tt.want {
				t.Errorf("MatchesEnv(%q) = %v, want %v", tt.req, got, tt.want)
			}
		})
	}
}

func TestValidateRequiredEnv(t *testing.T) {
	if err := ValidateRequiredEnv("REGION=us-east,CLUSTER=prod"); err != nil {
		t.Errorf("valid: %v", err)
	}
	if err := ValidateRequiredEnv(""); err != nil {
		t.Errorf("empty: %v", err)
	}
	if err := ValidateRequiredEnv("REGION_us-east"); err == nil {
		t.Error("expected error for malformed input")
	}
}
