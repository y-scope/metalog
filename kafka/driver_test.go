package kafka

import (
	"testing"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/metastore"
)

func TestRegisterDriver_And_GetDriver(t *testing.T) {
	// Register a test driver
	testFactory := func(tableName, tableID string, src *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (Adapter, error) {
		return nil, nil
	}
	RegisterDriver("test_driver", testFactory)

	factory, err := GetDriver("test_driver")
	if err != nil {
		t.Fatalf("GetDriver error: %v", err)
	}
	if factory == nil {
		t.Fatal("factory should not be nil")
	}
}

func TestGetDriver_Unknown(t *testing.T) {
	_, err := GetDriver("nonexistent_driver_xyz")
	if err == nil {
		t.Error("expected error for unknown driver")
	}
}

func TestRegisterDriver_Overwrite(t *testing.T) {
	called := false
	RegisterDriver("overwrite_test", func(tableName, tableID string, src *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (Adapter, error) {
		called = true
		return nil, nil
	})

	factory, err := GetDriver("overwrite_test")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = factory("", "", nil, nil, nil)
	if !called {
		t.Error("registered factory was not called")
	}
}
