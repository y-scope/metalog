// Command benchmark-ingestion measures end-to-end throughput of the gRPC push
// ingestion pipeline: producer goroutines → BatchingWriter channel → batch
// flush → MariaDB.
//
// It sweeps a matrix of concurrent producers × batch sizes to find the
// throughput ceiling under realistic load. Each cell starts a fresh
// BatchingWriter, submits all records, then stops the writer (flushing any
// remainder) and measures total elapsed time.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/docker/api/types/container"
	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mariadb"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/schema"
)

var log *zap.Logger

func main() {
	producers := flag.String("producers", "1,2,4,8,16,32", "CSV of concurrent producer counts")
	batchSizes := flag.String("batch-sizes", "10,50,100,500", "CSV of batch sizes (records per flush)")
	recordsPerProducer := flag.Int("records-per-producer", 500, "records each producer goroutine sends")
	tables := flag.Int("tables", 5, "number of distinct tables to spread records across")
	poolSize := flag.Int("pool-size", 32, "DB connection pool size")
	flag.Parse()

	log, _ = zap.NewProduction()
	defer log.Sync() //nolint:errcheck

	prodCounts := parseCSVInts(*producers)
	bsSizes := parseCSVInts(*batchSizes)

	ctx := context.Background()

	ctr, dsn := startMariaDB(ctx)
	defer func() { _ = ctr.Terminate(ctx) }()
	log.Info("MariaDB container ready")

	db := openDB(dsn, *poolSize)
	defer db.Close() //nolint:errcheck

	loadSchema(ctx, db)
	tableNames := createTestTables(ctx, db, *tables)

	fmt.Println()
	fmt.Println("╔════════════════════════════════════════════════╗")
	fmt.Println("║     INGESTION PIPELINE BENCHMARK               ║")
	fmt.Println("╚════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Printf("  Producers:           %s\n", *producers)
	fmt.Printf("  Batch sizes:         %s\n", *batchSizes)
	fmt.Printf("  Records/producer:    %d\n", *recordsPerProducer)
	fmt.Printf("  Tables:              %d\n", *tables)
	fmt.Println()

	results := make([][]float64, len(prodCounts))
	for i := range results {
		results[i] = make([]float64, len(bsSizes))
	}

	for pi, pc := range prodCounts {
		for bi, bs := range bsSizes {
			// Ensure at least 3 full batches per table so flushes are size-driven
			// rather than timer-driven (which would skew results for large batch sizes).
			rpp := *recordsPerProducer
			minPerTable := 3 * bs
			if perTable := rpp * pc / *tables; perTable < minPerTable {
				rpp = (minPerTable**tables + pc - 1) / pc
			}
			total := pc * rpp
			fmt.Printf("producers=%d  batch=%d  total=%d records ... ", pc, bs, total)
			tput := runBenchmark(ctx, db, tableNames, pc, bs, rpp)
			results[pi][bi] = tput
			fmt.Printf("%.0f records/sec\n", tput)
		}
	}

	printResultTable(prodCounts, bsSizes, results)
}

// runBenchmark spins up producerCount producers, each sending recordsPerProducer
// records through a shared BatchingWriter with the given batchSize. It measures
// true end-to-end commit throughput: from first submission to last DB flush,
// using IngestWithCallback to await every record's durable write confirmation.
func runBenchmark(
	ctx context.Context,
	db *sql.DB,
	tableNames []string,
	producerCount, batchSize, recordsPerProducer int,
) float64 {
	total := producerCount * recordsPerProducer

	// Single shared flushed channel — each committed record sends one signal.
	flushed := make(chan error, total)

	bw := ingestion.NewBatchingWriter(ctx, db, true, log,
		ingestion.WithBatchSize(batchSize),
	)
	// non-blocking service: producers spin on ErrChannelFull rather than
	// blocking indefinitely, keeping the benchmark from deadlocking.
	svc := ingestion.NewService(bw, false, log)

	var submitted atomic.Int64
	var wg sync.WaitGroup
	startCh := make(chan struct{})

	for p := 0; p < producerCount; p++ {
		wg.Add(1)
		go func(producerIdx int) {
			defer wg.Done()
			<-startCh
			now := time.Now().UnixNano()
			for i := 0; i < recordsPerProducer; i++ {
				tableName := tableNames[(producerIdx+i)%len(tableNames)]
				for {
					err := svc.IngestWithCallback(ctx, tableName, makeRecord(now), flushed)
					if err == nil {
						submitted.Add(1)
						break
					}
					if errors.Is(err, ingestion.ErrChannelFull) {
						runtime.Gosched() // yield and retry
						continue
					}
					break // unrecoverable, skip record
				}
			}
		}(p)
	}

	start := time.Now()
	close(startCh)
	wg.Wait()

	// Drain flushed signals: each committed record sends exactly one.
	n := int(submitted.Load())
	for i := 0; i < n; i++ {
		<-flushed
	}
	elapsed := time.Since(start)

	bw.Stop()
	return float64(n) / elapsed.Seconds()
}

// makeRecord returns a minimal but valid FileRecord for benchmarking.
// Uses StateIRBuffering with a synthetic IR path to satisfy DB constraints.
func makeRecord(now int64) *metastore.FileRecord {
	return &metastore.FileRecord{
		State:         metastore.StateIRBuffering,
		MinTimestamp:  now,
		MaxTimestamp:  now + int64(time.Second),
		RecordCount:   1,
		RetentionDays: 30,
		ExpiresAt:     now + 30*24*int64(time.Hour),
		ClpIRStorageBackend: sql.NullString{String: "fs", Valid: true},
		ClpIRBucket:         sql.NullString{String: "logs", Valid: true},
		ClpIRPath:           sql.NullString{String: "/bench/dummy.clp.zst", Valid: true},
		Dims:     map[string]any{},
		Aggs:     map[string]any{},
		Sketches: map[string][]byte{},
	}
}

func printResultTable(prodCounts, bsSizes []int, results [][]float64) {
	fmt.Println()
	fmt.Println("INGESTION THROUGHPUT (records/sec):")
	fmt.Printf("  (blocking submit → batch flush → MariaDB)\n")

	fmt.Printf("%-14s", "Producers")
	for _, bs := range bsSizes {
		fmt.Printf("%18s", fmt.Sprintf("batch=%d", bs))
	}
	fmt.Println()

	fmt.Printf("%-14s", "---------")
	for range bsSizes {
		fmt.Printf("%18s", "----------")
	}
	fmt.Println()

	for pi, pc := range prodCounts {
		fmt.Printf("%6d        ", pc)
		for bi := range bsSizes {
			fmt.Printf("%18.0f", results[pi][bi])
		}
		fmt.Println()
	}
	fmt.Println()
}

// --- Infrastructure helpers ---

func startMariaDB(ctx context.Context) (testcontainers.Container, string) {
	ctr, err := mariadb.Run(ctx,
		"mariadb:10.6",
		mariadb.WithDatabase("metalog_bench"),
		mariadb.WithUsername("root"),
		mariadb.WithPassword("password"),
		testcontainers.WithConfigModifier(func(c *container.Config) {
			// Raise connection limit; full durability (innodb default=1).
			c.Cmd = []string{
				"--max-connections=500",
				"--innodb-flush-log-at-trx-commit=1",
				"--sync-binlog=1",
			}
		}),
	)
	if err != nil {
		log.Fatal("start MariaDB", zap.Error(err))
	}
	connStr, err := ctr.ConnectionString(ctx, "parseTime=true", "interpolateParams=true")
	if err != nil {
		_ = ctr.Terminate(ctx)
		log.Fatal("get connection string", zap.Error(err))
	}
	return ctr, connStr
}

func openDB(dsn string, poolSize int) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("open database", zap.Error(err))
	}
	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(poolSize)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.PingContext(context.Background()); err != nil {
		log.Fatal("ping database", zap.Error(err))
	}
	return db
}

func loadSchema(ctx context.Context, db *sql.DB) {
	for _, stmt := range splitStatements(schema.SchemaSQL) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			log.Fatal("execute schema", zap.String("stmt", truncate(stmt, 120)), zap.Error(err))
		}
	}
}

func createTestTables(ctx context.Context, db *sql.DB, n int) []string {
	names := make([]string, n)
	for i := range names {
		name := fmt.Sprintf("bench_ingest_%03d", i)
		names[i] = name
		for _, q := range []string{
			fmt.Sprintf("INSERT IGNORE INTO _table (table_name, display_name) VALUES ('%s', '%s')", name, name),
			fmt.Sprintf("INSERT IGNORE INTO _table_config (table_name) VALUES ('%s')", name),
			fmt.Sprintf("INSERT IGNORE INTO _table_assignment (table_name) VALUES ('%s')", name),
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` LIKE _clp_template", name),
		} {
			if _, err := db.ExecContext(ctx, q); err != nil {
				log.Fatal("create test table", zap.String("table", name), zap.Error(err))
			}
		}
	}
	log.Info("test tables created", zap.Int("count", n))
	return names
}

func splitStatements(sqlText string) []string {
	lines := strings.Split(sqlText, "\n")
	var filtered []string
	for _, line := range lines {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			filtered = append(filtered, line)
		}
	}
	return strings.Split(strings.Join(filtered, "\n"), ";")
}

func parseCSVInts(s string) []int {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			log.Fatal("invalid integer in CSV", zap.String("value", p), zap.Error(err))
		}
		out = append(out, v)
	}
	return out
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

