package ingestion

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/encoding"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/schema"
)

// shutdownFlushTimeout is the deadline for the final flush when shutting down.
const shutdownFlushTimeout = 5 * time.Second

// tableWriter is a goroutine that batches and flushes records for a single table.
// It does not own a ColumnRegistry — it borrows one from the parent BatchingWriter
// on each flush to avoid data races and duplicate registry instances.
type tableWriter struct {
	tableName string
	isMariaDB bool
	ch        chan *metastore.FileRecord
	db        *sql.DB
	parent    *BatchingWriter
	fileRecs  *metastore.FileRecords
	log       *zap.Logger
}

// BatchingWriter manages per-table writer goroutines that batch records and
// flush them to the database using guarded UPSERTs.
type BatchingWriter struct {
	db        *sql.DB
	isMariaDB bool
	log       *zap.Logger

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
func NewBatchingWriter(ctx context.Context, db *sql.DB, isMariaDB bool, log *zap.Logger) *BatchingWriter {
	ctx, cancel := context.WithCancel(ctx)
	return &BatchingWriter{
		db:         db,
		isMariaDB:  isMariaDB,
		log:        log,
		writers:    make(map[string]*tableWriter),
		registries: make(map[string]*schema.ColumnRegistry),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// SetRegistry associates a column registry with a table. If a registry was
// already created lazily by ensureRegistry, the caller's instance replaces it
// (the coordinator's registry has alias refresh, recycler goroutine, etc.).
//
// This handoff is safe because the DB is the source of truth: any columns
// allocated by the lazy registry are persisted in _dim_registry/_agg_registry,
// so the coordinator's NewColumnRegistry loads them and its slot high-water
// marks are correct.
func (bw *BatchingWriter) SetRegistry(tableName string, reg *schema.ColumnRegistry) {
	bw.regMu.Lock()
	defer bw.regMu.Unlock()
	bw.registries[tableName] = reg
}

// ensureRegistry returns the registry for a table, creating one on-demand if
// it doesn't exist yet. This guarantees schema evolution (ALTER TABLE ADD
// COLUMN) happens on the first batch flush even if the coordinator unit hasn't
// started. When the coordinator later calls SetRegistry, its registry (with
// alias refresh, recycler, etc.) replaces the lazy one.
func (bw *BatchingWriter) ensureRegistry(ctx context.Context, tableName string) (*schema.ColumnRegistry, error) {
	bw.regMu.RLock()
	reg := bw.registries[tableName]
	bw.regMu.RUnlock()
	if reg != nil {
		return reg, nil
	}

	bw.regMu.Lock()
	defer bw.regMu.Unlock()

	// Double-check after acquiring write lock.
	if reg = bw.registries[tableName]; reg != nil {
		return reg, nil
	}

	reg, err := schema.NewColumnRegistry(ctx, bw.db, tableName, bw.isMariaDB, bw.log)
	if err != nil {
		return nil, fmt.Errorf("create column registry: %w", err)
	}
	bw.registries[tableName] = reg
	bw.log.Info("column registry created on-demand", zap.String("table", tableName))
	return reg, nil
}

// ErrChannelFull is returned when the per-table writer channel is at capacity.
// The gRPC handler maps this to RESOURCE_EXHAUSTED so clients retry with backoff.
var ErrChannelFull = fmt.Errorf("ingestion channel full")

// Submit sends a record to the appropriate per-table writer goroutine.
// If no writer exists for the table, one is created. Returns ErrChannelFull
// immediately if the channel is at capacity (non-blocking — used by gRPC).
func (bw *BatchingWriter) Submit(ctx context.Context, tableName string, rec *metastore.FileRecord) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	tw := bw.getOrCreateWriter(tableName)
	select {
	case tw.ch <- rec:
		return nil
	default:
		return ErrChannelFull
	}
}

// SubmitWait sends a record to the per-table writer goroutine, blocking
// until the channel has space or the context is cancelled. Used by the
// Kafka consumer to propagate backpressure without dropping messages.
func (bw *BatchingWriter) SubmitWait(ctx context.Context, tableName string, rec *metastore.FileRecord) error {
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

	fr, err := metastore.NewFileRecords(bw.db, tableName, bw.isMariaDB, bw.log)
	if err != nil {
		bw.log.Error("failed to create file records for table writer", zap.String("table", tableName), zap.Error(err))
		// Fall back to creating FileRecords per-flush.
		fr = nil
	}

	tw = &tableWriter{
		tableName: tableName,
		isMariaDB: bw.isMariaDB,
		ch:        make(chan *metastore.FileRecord, config.DefaultBatchSize),
		db:        bw.db,
		parent:    bw,
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
		fr, err = metastore.NewFileRecords(tw.db, tw.tableName, tw.isMariaDB, tw.log)
		if err != nil {
			tw.log.Error("failed to create file records", zap.Error(err))
			tw.notifyBatch(batch, err)
			return
		}
	}

	// Ensure column registry exists and resolve dims/aggs for the batch.
	// This triggers schema evolution (ALTER TABLE ADD COLUMN) on first flush.
	reg, err := tw.parent.ensureRegistry(ctx, tw.tableName)
	if err != nil {
		tw.log.Error("column registry unavailable", zap.Error(err))
		tw.notifyBatch(batch, err)
		return
	}

	if err := tw.resolveAndRemapBatch(ctx, reg, batch); err != nil {
		tw.log.Error("resolve columns failed", zap.Error(err))
		tw.notifyBatch(batch, err)
		return
	}

	dimCols := reg.ActiveDimColumns()
	aggCols := reg.ActiveAggColumns()
	floatAggCols := reg.FloatAggColumns()

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

// resolveAndRemapBatch batch-resolves all dim/agg logical keys in the batch to
// physical column names (allocating new columns if needed), then remaps each
// record's Dims/Aggs from logical keys to physical keys for UpsertBatch.
//
// Invariant: records produced by extractDims/extractAggs have matching
// Dims/DimMeta and Aggs/AggMeta entries. If a record has Dims but no DimMeta,
// the values will be dropped during remap (dimMap will be empty).
func (tw *tableWriter) resolveAndRemapBatch(ctx context.Context, reg *schema.ColumnRegistry, batch []*metastore.FileRecord) error {
	// Collect unique dim requests across the batch.
	dimSeen := make(map[string]struct{})
	var dimReqs []schema.DimRequest
	for _, rec := range batch {
		for _, dm := range rec.DimMeta {
			if _, ok := dimSeen[dm.Key]; ok {
				continue
			}
			dimSeen[dm.Key] = struct{}{}
			dimReqs = append(dimReqs, schema.DimRequest{
				DimKey:   dm.Key,
				BaseType: dm.BaseType,
				Width:    dm.Width,
			})
		}
	}

	// Collect unique agg requests across the batch.
	aggSeen := make(map[string]struct{})
	var aggReqs []schema.AggRequest
	for _, rec := range batch {
		for _, am := range rec.AggMeta {
			cacheKey := schema.AggCacheKey(am.Key, am.Value, am.Type)
			if _, ok := aggSeen[cacheKey]; ok {
				continue
			}
			aggSeen[cacheKey] = struct{}{}
			aggReqs = append(aggReqs, schema.AggRequest{
				AggKey:    am.Key,
				AggValue:  am.Value,
				AggType:   am.Type,
				ValueType: am.ValueType,
				AliasCol:  am.AliasCol,
			})
		}
	}

	// Batch-resolve dims (single ALTER TABLE for new columns).
	var dimMap map[string]string // logical key → physical col
	if len(dimReqs) > 0 {
		var err error
		dimMap, err = reg.ResolveOrAllocateDims(ctx, dimReqs)
		if err != nil {
			return fmt.Errorf("resolve dims: %w", err)
		}
	}

	// Batch-resolve aggs.
	var aggMap map[string]string // cache key → physical col
	if len(aggReqs) > 0 {
		var err error
		aggMap, err = reg.ResolveOrAllocateAggs(ctx, aggReqs)
		if err != nil {
			return fmt.Errorf("resolve aggs: %w", err)
		}
	}

	// Collect unique sketch keys across the batch.
	sketchSeen := make(map[string]struct{})
	var sketchKeys []string
	for _, rec := range batch {
		for key := range rec.Sketches {
			if _, ok := sketchSeen[key]; ok {
				continue
			}
			sketchSeen[key] = struct{}{}
			sketchKeys = append(sketchKeys, key)
		}
	}

	// Batch-resolve sketch keys to SET member names.
	var sketchMap map[string]string // logical key → SET member (e.g. "s03")
	if len(sketchKeys) > 0 {
		var err error
		sketchMap, err = reg.ResolveOrAllocateSketches(ctx, sketchKeys)
		if err != nil {
			return fmt.Errorf("resolve sketches: %w", err)
		}
	}

	// Remap each record from logical keys to physical column names.
	for _, rec := range batch {
		if len(rec.Dims) > 0 {
			phys := make(map[string]any, len(rec.Dims))
			for logicalKey, val := range rec.Dims {
				if col, ok := dimMap[logicalKey]; ok {
					phys[col] = val
				}
			}
			rec.Dims = phys
		}
		if len(rec.Aggs) > 0 {
			phys := make(map[string]any, len(rec.Aggs))
			for logicalKey, val := range rec.Aggs {
				if col, ok := aggMap[logicalKey]; ok {
					phys[col] = val
				}
			}
			rec.Aggs = phys
		}

		// Build SketchSetValue and ExtData for records with sketches.
		if len(rec.Sketches) > 0 {
			if err := encodeSketchExt(rec, sketchMap); err != nil {
				return fmt.Errorf("encode sketch ext: %w", err)
			}
		}
	}

	return nil
}

// encodeSketchExt builds the SketchSetValue (comma-joined SET members) and
// ExtData (LZ4-compressed msgpack) for a record's sketch data.
//
// The ext payload structure is:
//
//	{"sketches": {"uuid": {"type": "parquet_sbbf_xxhash64", "data": <bytes>}, ...}}
//
// Each sketch entry is decoded from the proto's msgpack-encoded BloomFilterSnapshot
// and nested under its logical key so readers can inspect the type field without
// full deserialization.
func encodeSketchExt(rec *metastore.FileRecord, sketchMap map[string]string) error {
	// Build SET value string from resolved member names.
	var members []string
	for key := range rec.Sketches {
		if member, ok := sketchMap[key]; ok {
			members = append(members, member)
		}
	}
	sort.Strings(members)
	rec.SketchSetValue = strings.Join(members, ",")

	// Build ext payload: decode each snapshot into a typed struct and nest
	// under sketches.<key>. Using a typed struct ensures the "type" and "data"
	// field names are preserved exactly through the msgpack round-trip.
	type sketchSnapshot struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	}
	sketchPayload := make(map[string]*sketchSnapshot, len(rec.Sketches))
	for key, data := range rec.Sketches {
		if _, ok := sketchMap[key]; !ok {
			continue // skip unresolved sketch keys
		}
		var snap sketchSnapshot
		if err := msgpack.Unmarshal(data, &snap); err != nil {
			return fmt.Errorf("decode sketch %q: %w", key, err)
		}
		sketchPayload[key] = &snap
	}

	type extPayloadType struct {
		Sketches map[string]*sketchSnapshot `msgpack:"sketches"`
	}
	extPayload := &extPayloadType{Sketches: sketchPayload}

	// Encode as LZ4-compressed msgpack using the shared codec.
	extData, err := encoding.Marshal(extPayload)
	if err != nil {
		return fmt.Errorf("encode ext: %w", err)
	}

	rec.ExtData = extData
	return nil
}

// notifyBatch sends the flush result to each record's Flushed channel.
// Uses non-blocking send to prevent stalling the tableWriter goroutine if
// a consumer has already returned (e.g., context expired).
func (tw *tableWriter) notifyBatch(batch []*metastore.FileRecord, err error) {
	for _, rec := range batch {
		if rec.Flushed != nil {
			select {
			case rec.Flushed <- err:
			default:
				tw.log.Debug("flush notification dropped, consumer already returned")
			}
		}
	}
}
