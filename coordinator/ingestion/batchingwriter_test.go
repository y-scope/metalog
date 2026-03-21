package ingestion

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/metastore"
)

// mockFlusher is a test mock for BatchFlusher.
type mockFlusher struct {
	mu        sync.Mutex
	calls     []flushCall
	flushFunc func(ctx context.Context, tableName string, batch []*metastore.FileRecord) error
}

type flushCall struct {
	tableName string
	count     int
}

func (m *mockFlusher) FlushBatch(ctx context.Context, tableName string, batch []*metastore.FileRecord) error {
	m.mu.Lock()
	m.calls = append(m.calls, flushCall{tableName: tableName, count: len(batch)})
	m.mu.Unlock()
	if m.flushFunc != nil {
		return m.flushFunc(ctx, tableName, batch)
	}
	return nil
}

func (m *mockFlusher) getCalls() []flushCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]flushCall, len(m.calls))
	copy(result, m.calls)
	return result
}

func (m *mockFlusher) totalRecords() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, c := range m.calls {
		total += c.count
	}
	return total
}

func makeRecord() *metastore.FileRecord {
	return &metastore.FileRecord{
		State:        metastore.StateIRClosed,
		MinTimestamp: time.Now().UnixNano(),
	}
}

func makeRecordWithFlush() (*metastore.FileRecord, chan error) {
	flushed := make(chan error, 1)
	rec := makeRecord()
	rec.Flushed = flushed
	return rec, flushed
}

func TestBatchingWriter_Submit_FlushAtBatchSize(t *testing.T) {
	mock := &mockFlusher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(3),
		WithFlushInterval(10*time.Second), // long interval — batch size should trigger
	)
	bw.testFlusher = mock

	// Submit exactly batch size records
	for i := 0; i < 3; i++ {
		if err := bw.Submit(ctx, "test_table", makeRecord()); err != nil {
			t.Fatalf("Submit error: %v", err)
		}
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	calls := mock.getCalls()
	if len(calls) == 0 {
		t.Fatal("expected at least one flush call")
	}
	if calls[0].count != 3 {
		t.Errorf("flush batch size = %d, want 3", calls[0].count)
	}
}

func TestBatchingWriter_Submit_FlushAtTimeout(t *testing.T) {
	mock := &mockFlusher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(1000),              // large batch — timeout should trigger
		WithFlushInterval(50*time.Millisecond),
	)
	bw.testFlusher = mock

	// Submit one record (below batch size)
	if err := bw.Submit(ctx, "test_table", makeRecord()); err != nil {
		t.Fatal(err)
	}

	// Wait for timeout flush
	time.Sleep(200 * time.Millisecond)

	if mock.totalRecords() != 1 {
		t.Errorf("flushed records = %d, want 1", mock.totalRecords())
	}
}

func TestBatchingWriter_NotifyOnSuccess(t *testing.T) {
	mock := &mockFlusher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(1),
		WithFlushInterval(10*time.Second),
	)
	bw.testFlusher = mock

	rec, flushed := makeRecordWithFlush()
	if err := bw.Submit(ctx, "test_table", rec); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-flushed:
		if err != nil {
			t.Errorf("expected nil flush error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for flush notification")
	}
}

func TestBatchingWriter_NotifyOnError(t *testing.T) {
	flushErr := fmt.Errorf("db connection lost")
	mock := &mockFlusher{
		flushFunc: func(_ context.Context, _ string, _ []*metastore.FileRecord) error {
			return flushErr
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(1),
		WithFlushInterval(10*time.Second),
	)
	bw.testFlusher = mock

	rec, flushed := makeRecordWithFlush()
	if err := bw.Submit(ctx, "test_table", rec); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-flushed:
		if err == nil {
			t.Error("expected flush error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for flush notification")
	}
}

func TestBatchingWriter_Submit_ChannelFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a flusher that blocks forever to keep the channel full
	var blocking atomic.Bool
	blocking.Store(true)
	mock := &mockFlusher{
		flushFunc: func(ctx context.Context, _ string, _ []*metastore.FileRecord) error {
			for blocking.Load() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
			}
			return nil
		},
	}

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(2),
		WithFlushInterval(10*time.Millisecond),
	)
	bw.testFlusher = mock

	// Fill the channel (capacity = batchSize = 2)
	bw.Submit(ctx, "t", makeRecord())
	bw.Submit(ctx, "t", makeRecord())

	// Wait for the batch to start flushing (blocking), then fill again
	time.Sleep(50 * time.Millisecond)
	bw.Submit(ctx, "t", makeRecord())
	bw.Submit(ctx, "t", makeRecord())

	// Next submit should get ErrChannelFull
	err := bw.Submit(ctx, "t", makeRecord())
	if err != ErrChannelFull {
		t.Errorf("expected ErrChannelFull, got %v", err)
	}

	blocking.Store(false)
}

func TestBatchingWriter_Stop_DrainsAndFlushes(t *testing.T) {
	flushCount := atomic.Int64{}
	mock := &mockFlusher{
		flushFunc: func(_ context.Context, _ string, batch []*metastore.FileRecord) error {
			flushCount.Add(int64(len(batch)))
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(100),                // large batch — won't trigger by size
		WithFlushInterval(10*time.Second), // long interval — won't trigger by time
	)
	bw.testFlusher = mock

	// Submit a few records
	for i := 0; i < 5; i++ {
		bw.Submit(ctx, "test_table", makeRecord())
	}

	// Stop should drain and flush remaining records
	bw.Stop()

	if flushCount.Load() < 5 {
		t.Errorf("flushed %d records on shutdown, want >= 5", flushCount.Load())
	}
}

func TestBatchingWriter_SubmitWait_Blocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock := &mockFlusher{}
	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(1),
		WithFlushInterval(50*time.Millisecond),
	)
	bw.testFlusher = mock

	// Fill the channel
	bw.Submit(ctx, "t", makeRecord())

	// SubmitWait should block briefly then succeed after the flush timer fires
	done := make(chan error, 1)
	go func() {
		done <- bw.SubmitWait(ctx, "t", makeRecord())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("SubmitWait error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitWait blocked forever")
	}
}

func TestBatchingWriter_MultipleTablesRouteCorrectly(t *testing.T) {
	mock := &mockFlusher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bw := NewBatchingWriter(ctx, nil, false, zap.NewNop(),
		WithBatchSize(2),
		WithFlushInterval(50*time.Millisecond),
	)
	bw.testFlusher = mock

	// Submit to two different tables
	bw.Submit(ctx, "table_a", makeRecord())
	bw.Submit(ctx, "table_a", makeRecord())
	bw.Submit(ctx, "table_b", makeRecord())

	time.Sleep(200 * time.Millisecond)

	calls := mock.getCalls()
	tableACounts := 0
	tableBCounts := 0
	for _, c := range calls {
		if c.tableName == "table_a" {
			tableACounts += c.count
		} else if c.tableName == "table_b" {
			tableBCounts += c.count
		}
	}

	if tableACounts != 2 {
		t.Errorf("table_a records = %d, want 2", tableACounts)
	}
	if tableBCounts != 1 {
		t.Errorf("table_b records = %d, want 1", tableBCounts)
	}
}
