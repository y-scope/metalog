// Command benchmark-taskqueue sweeps a matrix of node counts × workers-per-node
// to find the throughput ceiling of the _task_queue table under concurrent load.
//
// Each node has 1 prefetcher goroutine (the sole DB claimer) and N worker
// goroutines consuming from a shared channel — matching the production architecture.
// Batch size = workers-per-node × batch-multiplier.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mariadb"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/ddl"
	"github.com/y-scope/metalog/taskqueue"
)

var logger *zap.Logger

func main() {
	var (
		nodes          = flag.String("nodes", "1,2,4,8,16,32", "CSV of node counts")
		wpn            = flag.String("workers-per-node", "1,2,4,8,16,32", "CSV of workers-per-node counts")
		batchMult      = flag.Int("batch-multiplier", 1, "batch = workers-per-node * multiplier")
		tasksPerWorker = flag.Int("tasks-per-worker", 50, "tasks per worker goroutine")
		tables         = flag.Int("tables", 10, "number of tables to spread tasks across")
		poolSize       = flag.Int("pool-size", 200, "DB connection pool size")
	)
	flag.Parse()

	logger, _ = zap.NewProduction()
	defer logger.Sync()

	nodeCounts := parseCSVInts(*nodes)
	wpnCounts := parseCSVInts(*wpn)

	ctx := context.Background()

	schemaFile := writeSchemaTempFile()
	defer os.Remove(schemaFile)

	ctr, dsn := startMariaDB(ctx, schemaFile)
	defer ctr.Terminate(ctx)
	logger.Info("MariaDB container ready")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logger.Fatal("open database", zap.Error(err))
	}
	defer db.Close()

	db.SetMaxOpenConns(*poolSize)
	db.SetMaxIdleConns(*poolSize)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		logger.Fatal("ping database", zap.Error(err))
	}

	// Bootstrap schema.
	if err := execSchema(ctx, db); err != nil {
		logger.Fatal("execute schema", zap.Error(err))
	}

	// Create table registry entries for FK constraints.
	tableNames := make([]string, *tables)
	for i := range tableNames {
		tableNames[i] = fmt.Sprintf("bench_table_%03d", i)
	}
	if err := ensureTableEntries(ctx, db, tableNames); err != nil {
		logger.Fatal("create table entries", zap.Error(err))
	}

	tq := taskqueue.NewQueue(db, logger)

	// Print header.
	fmt.Println()
	fmt.Println("\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557")
	fmt.Println("\u2551    TASK QUEUE SCALABILITY BENCHMARK            \u2551")
	fmt.Println("\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d")
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Printf("  Nodes:            %s\n", *nodes)
	fmt.Printf("  Workers per node: %s\n", *wpn)
	fmt.Printf("  Batch multiplier: %d\n", *batchMult)
	fmt.Printf("  Tasks per worker: %d\n", *tasksPerWorker)
	fmt.Printf("  Tables:           %d\n", *tables)
	fmt.Println()

	// Result matrices: [nodeIdx][wpnIdx] -> throughput.
	claimResults := make([][]float64, len(nodeCounts))
	completeResults := make([][]float64, len(nodeCounts))
	for i := range claimResults {
		claimResults[i] = make([]float64, len(wpnCounts))
		completeResults[i] = make([]float64, len(wpnCounts))
	}

	for ni, nc := range nodeCounts {
		for wi, w := range wpnCounts {
			batchSize := w * *batchMult
			totalWorkers := nc * w
			totalTasks := *tasksPerWorker * totalWorkers
			fmt.Printf("Running nodes=%d workers/node=%d (total=%d) batch=%d totalTasks=%d ... ",
				nc, w, totalWorkers, batchSize, totalTasks)

			// Reset: delete all tasks, then seed.
			if _, err := db.ExecContext(ctx, "DELETE FROM _task_queue"); err != nil {
				logger.Fatal("reset task queue", zap.Error(err))
			}
			if err := seedTasks(ctx, db, tableNames, totalTasks); err != nil {
				logger.Fatal("seed tasks", zap.Error(err))
			}

			// CLAIM phase: each node has 1 prefetcher + N workers.
			claimThroughput, allClaimed := runClaimPhase(ctx, tq, nc, w, *tasksPerWorker, batchSize)
			claimResults[ni][wi] = claimThroughput

			// COMPLETE phase.
			completeThroughput := runCompletePhase(ctx, tq, allClaimed)
			completeResults[ni][wi] = completeThroughput

			fmt.Printf("claim=%.0f tasks/sec  complete=%.0f tasks/sec\n", claimThroughput, completeThroughput)
		}
	}

	// Print result tables.
	printResultTable("CLAIM THROUGHPUT (tasks/sec):", nodeCounts, wpnCounts, claimResults, *batchMult)
	printResultTable("COMPLETE THROUGHPUT (tasks/sec):", nodeCounts, wpnCounts, completeResults, *batchMult)
}

// runClaimPhase spawns nodeCount nodes, each with 1 prefetcher goroutine and
// workersPerNode worker goroutines consuming from the prefetcher's channel.
func runClaimPhase(
	ctx context.Context,
	tq *taskqueue.Queue,
	nodeCount, workersPerNode, tasksPerWorker, batchSize int,
) (float64, [][]*taskqueue.Task) {
	allClaimed := make([][]*taskqueue.Task, nodeCount)
	var mu sync.Mutex
	var wg sync.WaitGroup
	startCh := make(chan struct{})

	for n := 0; n < nodeCount; n++ {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			<-startCh

			nodeID := fmt.Sprintf("node-%04d", nodeIdx)

			// Buffered channel: prefetcher feeds, workers consume.
			taskCh := make(chan *taskqueue.Task, batchSize*2)
			totalNeeded := tasksPerWorker * workersPerNode

			// 1 prefetcher goroutine for this node.
			go func() {
				defer close(taskCh)

				claimed := 0
				for claimed < totalNeeded {
					needed := totalNeeded - claimed
					bs := batchSize
					if bs > needed {
						bs = needed
					}
					tasks, err := tq.ClaimTasks(ctx, "", nodeID, bs)
					if err != nil {
						logger.Warn("claim error", zap.Int("node", nodeIdx), zap.Error(err))
						continue
					}
					if len(tasks) == 0 {
						break
					}
					for _, t := range tasks {
						taskCh <- t
					}
					claimed += len(tasks)
				}
			}()

			// N worker goroutines consume from the channel.
			var workerWg sync.WaitGroup
			var nodeTasks []*taskqueue.Task
			var nodeMu sync.Mutex

			for w := 0; w < workersPerNode; w++ {
				workerWg.Add(1)
				go func() {
					defer workerWg.Done()
					var local []*taskqueue.Task
					for t := range taskCh {
						local = append(local, t)
					}
					nodeMu.Lock()
					nodeTasks = append(nodeTasks, local...)
					nodeMu.Unlock()
				}()
			}

			workerWg.Wait()

			mu.Lock()
			allClaimed[nodeIdx] = nodeTasks
			mu.Unlock()
		}(n)
	}

	start := time.Now()
	close(startCh)
	wg.Wait()
	elapsed := time.Since(start)

	totalClaimed := 0
	for _, c := range allClaimed {
		totalClaimed += len(c)
	}

	return float64(totalClaimed) / elapsed.Seconds(), allClaimed
}

// runCompletePhase spawns goroutines to complete all claimed tasks.
func runCompletePhase(ctx context.Context, tq *taskqueue.Queue, allClaimed [][]*taskqueue.Task) float64 {
	totalTasks := 0
	for _, c := range allClaimed {
		totalTasks += len(c)
	}
	if totalTasks == 0 {
		return 0
	}

	var wg sync.WaitGroup
	startCh := make(chan struct{})

	for _, tasks := range allClaimed {
		if len(tasks) == 0 {
			continue
		}
		wg.Add(1)
		go func(tasks []*taskqueue.Task) {
			defer wg.Done()
			<-startCh
			for _, t := range tasks {
				if _, err := tq.CompleteTask(ctx, t.TaskID, nil); err != nil {
					logger.Warn("complete error", zap.Int64("taskID", t.TaskID), zap.Error(err))
				}
			}
		}(tasks)
	}

	start := time.Now()
	close(startCh)
	wg.Wait()
	elapsed := time.Since(start)

	return float64(totalTasks) / elapsed.Seconds()
}

// --- Helper functions ---

func execSchema(ctx context.Context, db *sql.DB) error {
	stmts := splitStatements(ddl.SQL)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			if strings.Contains(stmt, "IF EXISTS") || strings.Contains(stmt, "IF NOT EXISTS") {
				continue
			}
			return fmt.Errorf("exec schema statement: %w\nSQL: %s", err, truncate(stmt, 200))
		}
	}
	return nil
}

func splitStatements(sqlText string) []string {
	lines := strings.Split(sqlText, "\n")
	var filtered []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		filtered = append(filtered, line)
	}
	content := strings.Join(filtered, "\n")
	return strings.Split(content, ";")
}

func ensureTableEntries(ctx context.Context, db *sql.DB, tableNames []string) error {
	for _, tn := range tableNames {
		_, err := db.ExecContext(ctx,
			"INSERT IGNORE INTO _table (table_name, display_name) VALUES (?, ?)",
			tn, tn)
		if err != nil {
			return fmt.Errorf("insert _table entry %q: %w", tn, err)
		}
	}
	return nil
}

func seedTasks(ctx context.Context, db *sql.DB, tableNames []string, totalTasks int) error {
	payload := []byte("benchmark-payload")
	const batchInsertSize = 500

	for i := 0; i < totalTasks; i += batchInsertSize {
		end := i + batchInsertSize
		if end > totalTasks {
			end = totalTasks
		}
		n := end - i
		nowNano := time.Now().UnixNano()

		var sb strings.Builder
		sb.WriteString("INSERT INTO _task_queue (table_name, created_at, version, input) VALUES ")
		args := make([]any, 0, n*4)
		for j := 0; j < n; j++ {
			if j > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("(?,?,?,?)")
			tn := tableNames[(i+j)%len(tableNames)]
			args = append(args, tn, nowNano, taskqueue.TaskPayloadVersion, payload)
		}

		if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("seed batch at offset %d: %w", i, err)
		}
	}
	return nil
}

func printResultTable(title string, nodeCounts, wpnCounts []int, results [][]float64, batchMult int) {
	fmt.Println()
	fmt.Println(title)
	fmt.Printf("  (batch = workers-per-node × %d)\n", batchMult)

	// Header row.
	fmt.Printf("%-12s", "Nodes")
	for _, w := range wpnCounts {
		fmt.Printf("%20s", fmt.Sprintf("%d workers/node", w))
	}
	fmt.Println()

	// Separator.
	fmt.Printf("%-12s", "------")
	for range wpnCounts {
		fmt.Printf("%20s", "------------")
	}
	fmt.Println()

	// Data rows.
	for ni, nc := range nodeCounts {
		fmt.Printf("%6d      ", nc)
		for wi := range wpnCounts {
			fmt.Printf("%20.0f", results[ni][wi])
		}
		fmt.Println()
	}
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
			logger.Fatal("invalid integer in CSV", zap.String("value", p), zap.Error(err))
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

// startMariaDB launches a MariaDB container with max_connections=2000 and returns
// the container handle and a DSN connection string. Schema is loaded via init script.
func startMariaDB(ctx context.Context, schemaFile string) (testcontainers.Container, string) {
	container, err := mariadb.Run(ctx,
		"mariadb:10.6",
		mariadb.WithDatabase("metalog_metastore"),
		mariadb.WithUsername("root"),
		mariadb.WithPassword("password"),
		mariadb.WithScripts(schemaFile),
		testcontainers.WithConfigModifier(func(c *container.Config) {
			c.Cmd = []string{"--max-connections=2000", "--innodb-flush-log-at-trx-commit=1"}
		}),
	)
	if err != nil {
		logger.Fatal("start MariaDB container", zap.Error(err))
	}

	connStr, err := container.ConnectionString(ctx, "parseTime=true", "interpolateParams=true")
	if err != nil {
		container.Terminate(ctx)
		logger.Fatal("get connection string", zap.Error(err))
	}
	return container, connStr
}

// writeSchemaTempFile writes the embedded DDL to a temp file for the MariaDB
// init script and returns its path.
func writeSchemaTempFile() string {
	f, err := os.CreateTemp("", "schema-*.sql")
	if err != nil {
		logger.Fatal("create temp schema file", zap.Error(err))
	}
	if _, err := f.WriteString(ddl.SQL); err != nil {
		f.Close()
		logger.Fatal("write schema file", zap.Error(err))
	}
	f.Close()
	return f.Name()
}
