package consolidation

import (
	"testing"
	"time"
)

func TestCreatePolicy_TimeWindow(t *testing.T) {
	p, err := CreatePolicy(PolicyConfig{Type: "time_window", WindowSize: time.Hour, MinFiles: 2, MaxFiles: 50})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*TimeWindowPolicy); !ok {
		t.Errorf("expected *TimeWindowPolicy, got %T", p)
	}
}

func TestCreatePolicy_Default(t *testing.T) {
	p, err := CreatePolicy(PolicyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*TimeWindowPolicy); !ok {
		t.Errorf("empty type should default to TimeWindowPolicy, got %T", p)
	}
}

func TestCreatePolicy_SparkJob(t *testing.T) {
	p, err := CreatePolicy(PolicyConfig{Type: "spark_job", GroupingDimKey: "app_id"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*SparkJobPolicy); !ok {
		t.Errorf("expected *SparkJobPolicy, got %T", p)
	}
}

func TestCreatePolicy_SparkJob_MissingKey(t *testing.T) {
	_, err := CreatePolicy(PolicyConfig{Type: "spark_job"})
	if err == nil {
		t.Error("expected error for spark_job without groupingDimKey")
	}
}

func TestCreatePolicy_Audit(t *testing.T) {
	p, err := CreatePolicy(PolicyConfig{Type: "audit", MinFiles: 1, MaxFiles: 500})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*AuditPolicy); !ok {
		t.Errorf("expected *AuditPolicy, got %T", p)
	}
}

func TestCreatePolicy_Unknown(t *testing.T) {
	_, err := CreatePolicy(PolicyConfig{Type: "nonexistent"})
	if err == nil {
		t.Error("expected error for unknown policy type")
	}
}
