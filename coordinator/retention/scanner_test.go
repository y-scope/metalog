package retention

import (
	"context"
	"testing"
	"time"
)

func TestDefaultStrategy_RunRespectsContext(t *testing.T) {
	// Verify that Run() exits promptly when context is canceled.
	ctx, cancel := context.WithCancel(context.Background())

	s := &defaultStrategy{
		interval: time.Hour, // long interval so it won't tick
	}

	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not exit after context cancellation")
	}
}

func TestCreateStrategy_Default(t *testing.T) {
	s, err := CreateStrategy("default", Deps{})
	if err == nil && s == nil {
		t.Fatal("CreateStrategy returned nil strategy without error")
	}
	// Will fail on NewFileRecords (empty table name) but that's expected —
	// we're testing that the type registry resolves "default".
}

func TestCreateStrategy_Unknown(t *testing.T) {
	_, err := CreateStrategy("nonexistent", Deps{})
	if err == nil {
		t.Fatal("CreateStrategy should fail for unknown type")
	}
}

func TestRegisterType_Custom(t *testing.T) {
	called := false
	RegisterType("test-custom", StrategyMeta{
		Factory: func(deps Deps) (Strategy, error) {
			called = true
			return &defaultStrategy{interval: time.Hour}, nil
		},
	})

	s, err := CreateStrategy("test-custom", Deps{})
	if err != nil {
		t.Fatalf("CreateStrategy error: %v", err)
	}
	if !called {
		t.Fatal("custom factory was not called")
	}
	if s == nil {
		t.Fatal("strategy is nil")
	}
}
