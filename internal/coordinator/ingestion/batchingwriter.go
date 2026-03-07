package ingestion

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/schema"
)

// shutdownFlushTimeout is the deadline for the final flush when shutting down.
const shutdownFlushTimeout = 5 * time.Second

// tableWriter is a goroutine that batches and flushes records for a single table.
type tableWriter struct {
	tableName string
	ch        chan *metastore.FileRecord
	db        *sql.DB
	registry  *schema.ColumnRegistry
	fileRecs  *metastore.FileRecords
	log       *zap.Logger
}

// BatchingWriter manages per-table writer goroutines that batch records and
// flush them to the database using guarded UPSERTs.
type BatchingWriter struct {
	db  *sql.DB
	log *zap.Logger

	mu      sync.RWMutex
	writers map[string]*tableWriter
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// registries provides column registry per table
	registries map[string]*schema.ColumnRegistry
	regMu      sync.RWMutex
}

// NewBatchingWriter creates a BatchingWriter.
func NewBatchingWriter(ctx context.Context, db *sql.DB, log *zap.Logger) *BatchingWriter {
	ctx, cancel := context.WithCancel(ctx)
	return &BatchingWriter{
		db:         db,
		log:        log,
		writers:    make(map[string]*tableWriter),
		registries: make(map[string]*schema.ColumnRegistry),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// SetRegistry associates a column registry with a table.
func (bw *BatchingWriter) SetRegistry(tableName string, reg *schema.ColumnRegistry) {
	bw.regMu.Lock()
	defer bw.regMu.Unlock()
	bw.registries[tableName] = reg
}

// Submit sends a record to the appropriate per-table writer goroutine.
// If no writer exists for the table, one is created.
func (bw *BatchingWriter) Submit(ctx context.Context, tableName string, rec *metastore.FileRecord) error {
	tw := bw.getOrCreateWriter(tableName)
	select {
	case tw.ch <- rec:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop cancels all writer goroutines and waits for them to flush.
func (bw *BatchingWriter) Stop() {
	bw.cancel()
	bw.wg.Wait()
}

func (bw *BatchingWriter) getOrCreateWriter(tableName string) *tableWriter {
	bw.mu.RLock()
	tw, ok := bw.writers[tableName]
	bw.mu.RUnlock()
	if ok {
		return tw
	}

	bw.mu.Lock()
	defer bw.mu.Unlock()

	// Double-check
	if tw, ok = bw.writers[tableName]; ok {
		return tw
	}

	bw.regMu.RLock()
	reg := bw.registries[tableName]
	bw.regMu.RUnlock()

	fr, err := metastore.NewFileRecords(bw.db, tableName, bw.log)
	if err != nil {
		bw.log.Error("failed to create file records for table writer", zap.String("table", tableName), zap.Error(err))
		// Fall back to creating FileRecords per-flush.
		fr = nil
	}

	tw = &tableWriter{
		tableName: tableName,
		ch:        make(chan *metastore.FileRecord, config.DefaultBatchSize),
		db:        bw.db,
		registry:  reg,
		fileRecs:  fr,
		log:       bw.log.With(zap.String("table", tableName)),
	}
	bw.writers[tableName] = tw

	bw.wg.Add(1)
	go func() {
		defer bw.wg.Done()
		tw.run(bw.ctx)
	}()

	return tw
}

func (tw *tableWriter) run(ctx context.Context) {
	ticker := time.NewTicker(config.DefaultBatchFlushInterval)
	defer ticker.Stop()

	batch := make([]*metastore.FileRecord, 0, config.DefaultBatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		tw.flushBatch(ctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case rec := <-tw.ch:
			batch = append(batch, rec)
			if len(batch) >= config.DefaultBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			// Drain remaining records from the channel.
			for {
				select {
				case rec := <-tw.ch:
					batch = append(batch, rec)
				default:
					// Flush what we can with a short deadline.
					flushCtx, flushCancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
					tw.flushBatch(flushCtx, batch)
					flushCancel()
					batch = batch[:0]

					// Any records that arrive after this drain are orphaned.
					// Drain once more and notify them with an error.
					shutdownErr := fmt.Errorf("coordinator shutting down")
					for {
						select {
						case rec := <-tw.ch:
							if rec.Flushed != nil {
								rec.Flushed <- shutdownErr
							}
						default:
							return
						}
					}
				}
			}
		}
	}
}

func (tw *tableWriter) flushBatch(ctx context.Context, batch []*metastore.FileRecord) {
	fr := tw.fileRecs
	if fr == nil {
		var err error
		fr, err = metastore.NewFileRecords(tw.db, tw.tableName, tw.log)
		if err != nil {
			tw.log.Error("failed to create file records", zap.Error(err))
			tw.notifyBatch(batch, err)
			return
		}
	}

	var dimCols, aggCols []string
	var floatAggCols map[string]bool
	if tw.registry != nil {
		dimCols = tw.registry.ActiveDimColumns()
		aggCols = tw.registry.ActiveAggColumns()
		floatAggCols = tw.registry.FloatAggColumns()
	}

	n, err := fr.UpsertBatch(ctx, batch, dimCols, aggCols, floatAggCols)
	if err != nil {
		tw.log.Error("batch upsert failed",
			zap.Int("batchSize", len(batch)),
			zap.Error(err),
		)
		tw.notifyBatch(batch, err)
		return
	}

	tw.log.Debug("flushed batch",
		zap.Int("records", len(batch)),
		zap.Int64("rowsAffected", n),
	)

	tw.notifyBatch(batch, nil)
}

// notifyBatch sends the flush result to each record's Flushed channel.
func (tw *tableWriter) notifyBatch(batch []*metastore.FileRecord, err error) {
	for _, rec := range batch {
		if rec.Flushed != nil {
			rec.Flushed <- err
		}
	}
}
