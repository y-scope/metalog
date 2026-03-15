package consolidation

import (
	"encoding/json"
	"testing"
)

func TestCreatePolicy_TimeWindow(t *testing.T) {
	cfg := json.RawMessage(`{"window_size": "1h", "min_files": 2, "max_files": 50}`)
	p, err := CreatePolicy("time_window", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*TimeWindowPolicy); !ok {
		t.Errorf("expected *TimeWindowPolicy, got %T", p)
	}
}

func TestCreatePolicy_TimeWindow_Defaults(t *testing.T) {
	p, err := CreatePolicy("time_window", nil)
	if err != nil {
		t.Fatal(err)
	}
	tw, ok := p.(*TimeWindowPolicy)
	if !ok {
		t.Fatalf("expected *TimeWindowPolicy, got %T", p)
	}
	if tw.MinFilesPerGroup != defaultMinFilesPerGroup {
		t.Errorf("MinFilesPerGroup = %d, want %d", tw.MinFilesPerGroup, defaultMinFilesPerGroup)
	}
	if tw.MaxFilesPerGroup != defaultMaxFilesPerGroup {
		t.Errorf("MaxFilesPerGroup = %d, want %d", tw.MaxFilesPerGroup, defaultMaxFilesPerGroup)
	}
}

func TestCreatePolicy_Default(t *testing.T) {
	p, err := CreatePolicy("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*TimeWindowPolicy); !ok {
		t.Errorf("empty type should default to TimeWindowPolicy, got %T", p)
	}
}

func TestCreatePolicy_SparkJob(t *testing.T) {
	cfg := json.RawMessage(`{"grouping_dim_key": "app_id"}`)
	p, err := CreatePolicy("spark_job", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*SparkJobPolicy); !ok {
		t.Errorf("expected *SparkJobPolicy, got %T", p)
	}
}

func TestCreatePolicy_SparkJob_MissingKey(t *testing.T) {
	_, err := CreatePolicy("spark_job", nil)
	if err == nil {
		t.Error("expected error for spark_job without grouping_dim_key")
	}
}

func TestCreatePolicy_Unknown(t *testing.T) {
	_, err := CreatePolicy("nonexistent", nil)
	if err == nil {
		t.Error("expected error for unknown policy type")
	}
}
