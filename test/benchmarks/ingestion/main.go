// Package main implements a unified ingestion benchmark for the metalog coordinator.
// It supports three modes:
//   - grpc:        Send records via MetadataIngestionService.Ingest RPC
//   - kafka-proto: Produce protobuf-encoded records to Kafka
//   - kafka-json:  Produce JSON-encoded records to Kafka
//
// Infrastructure (MariaDB, Kafka) is started via testcontainers and a coordinator
// is run in-process.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/mariadb"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/coordinator"
	coordinatorpb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/grpcserver"
	metalogkafka "github.com/y-scope/metalog/kafka/franzgo"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/node"
	"github.com/y-scope/metalog/schema"
)

var (
	zones  = []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}
	logger *zap.Logger
)

func main() {
	mode := flag.String("mode", "grpc", "Benchmark mode: grpc, kafka-proto, kafka-json")
	records := flag.Int("records", 100000, "Number of records to send")
	apps := flag.Int("apps", 10000, "Number of distinct app IDs")
	table := flag.String("table", "clp_spark", "Target table name")
	concurrency := flag.Int("concurrency", 5000, "Max concurrent in-flight RPCs (grpc mode)")
	blocking := flag.Bool("blocking", true, "Use blocking ingestion (grpc mode). true=higher throughput, false=RESOURCE_EXHAUSTED on full channel")
	batchSize := flag.Int("batch-size", 1000, "Kafka producer batch size")
	partitions := flag.Int("partitions", 2, "Kafka topic partitions")
	timeout := flag.Int("timeout", 120, "Timeout in seconds for DB convergence")

	flag.Parse()

	logger, _ = zap.NewProduction()
	defer logger.Sync()

	ctx := context.Background()

	// Start MariaDB.
	logger.Info("starting MariaDB container")
	schemaFile := writeSchemaTempFile()
	defer os.Remove(schemaFile)

	mc, err := mariadb.Run(ctx, "mariadb:10.6",
		mariadb.WithDatabase("metalog_metastore"),
		mariadb.WithUsername("root"),
		mariadb.WithPassword("password"),
		mariadb.WithScripts(schemaFile),
	)
	if err != nil {
		logger.Fatal("start MariaDB", zap.Error(err))
	}
	defer mc.Terminate(ctx)

	connStr, err := mc.ConnectionString(ctx, "parseTime=true", "interpolateParams=true")
	if err != nil {
		logger.Fatal("MariaDB connection string", zap.Error(err))
	}
	logger.Info("MariaDB ready")

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		logger.Fatal("open database", zap.Error(err))
	}
	defer db.Close()
	db.SetMaxOpenConns(10)

	// Get MariaDB host:port for config.
	dbEndpoint, err := mc.Endpoint(ctx, "")
	if err != nil {
		logger.Fatal("MariaDB endpoint", zap.Error(err))
	}
	dbHostResolved, dbPortStr, _ := net.SplitHostPort(dbEndpoint)
	var dbPortResolved int
	fmt.Sscanf(dbPortStr, "%d", &dbPortResolved)

	// Start Kafka if needed.
	var kafkaBootstrap string
	if *mode == "kafka-proto" || *mode == "kafka-json" {
		logger.Info("starting Kafka container")
		kc, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
		if err != nil {
			logger.Fatal("start Kafka", zap.Error(err))
		}
		defer kc.Terminate(ctx)

		brokers, err := kc.Brokers(ctx)
		if err != nil {
			logger.Fatal("Kafka brokers", zap.Error(err))
		}
		kafkaBootstrap = brokers[0]
		logger.Info("Kafka ready", zap.String("bootstrap", kafkaBootstrap))

		// Create topic via franz-go admin client.
		adminClient, err := kgo.NewClient(kgo.SeedBrokers(kafkaBootstrap))
		if err != nil {
			logger.Fatal("create admin client", zap.Error(err))
		}
		admin := kadm.NewClient(adminClient)
		createResp, err := admin.CreateTopics(ctx, int32(*partitions), 1, nil, *table)
		if err != nil {
			logger.Fatal("create topic", zap.Error(err))
		}
		for _, r := range createResp.Sorted() {
			if r.Err != nil {
				logger.Fatal("create topic failed", zap.String("topic", r.Topic), zap.Error(r.Err))
			}
		}
		adminClient.Close()
		logger.Info("topic created", zap.String("topic", *table), zap.Int("partitions", *partitions))
	}

	// For Kafka modes: produce all records BEFORE starting coordinator.
	// This isolates consumer→DB throughput from producer speed.
	isKafka := *mode == "kafka-proto" || *mode == "kafka-json"
	if isKafka {
		useJSON := *mode == "kafka-json"
		logger.Info("pre-producing records to Kafka", zap.Int("records", *records))
		produceToKafka(kafkaBootstrap, *table, *table, *records, *apps, *batchSize, useJSON)
		logger.Info("all records in Kafka, starting coordinator to measure drain throughput")
	}

	// Find a free port for gRPC.
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		logger.Fatal("listen for gRPC", zap.Error(err))
	}
	grpcPortResolved := grpcListener.Addr().(*net.TCPAddr).Port
	grpcListener.Close()

	// Build node config programmatically.
	cfg := &config.NodeConfig{
		Database: config.DatabaseSection{
			Primary: config.DatabaseConfig{
				Host:        dbHostResolved,
				Port:        dbPortResolved,
				Database:    "metalog_metastore",
				User:        "root",
				Password:    "password",
				PoolSize:    5,
				PoolMinIdle: 2,
			},
		},
		Coordinator: config.CoordinatorConfig{
			Enabled:                       true,
			HAStrategy:                    config.HAStrategyHeartbeat,
			HeartbeatIntervalSeconds:      30,
			DeadNodeThresholdSeconds:      180,
			ReconciliationIntervalSeconds: 2,
		},
		GRPC: config.GRPCConfig{
			Port:              grpcPortResolved,
			Ingestion:         *mode == "grpc",
			Admin:             true,
			BlockingIngestion: blocking,
		},
	}

	// Start node.
	logger.Info("starting coordinator node")
	n, err := node.NewNode(cfg, logger, node.WithKafkaAdapterFactory(metalogkafka.NewDefaultAdapterFactory(nil)))
	if err != nil {
		logger.Fatal("create node", zap.Error(err))
	}
	if err := n.Start(); err != nil {
		logger.Fatal("start node", zap.Error(err))
	}
	defer n.Stop()

	// Start gRPC server.
	grpcSrv := grpcserver.NewServer(grpcPortResolved, logger)
	if *mode == "grpc" {
		ingestionHandler := grpcserver.NewIngestionHandler(n.IngestionService(), logger)
		pb.RegisterMetadataIngestionServiceServer(grpcSrv.GRPCServer(), ingestionHandler)
	}
	regSvc := coordinator.NewTableRegistration(n.Shared().DB, n.Shared().IsMariaDB, cfg.Coordinator.TableCompression, logger)
	kafkaSources := metastore.NewKafkaSourceStore(n.Shared().DB, logger)
	adminHandler := grpcserver.NewAdminHandler(regSvc, kafkaSources, logger)
	coordinatorpb.RegisterAdminServiceServer(grpcSrv.GRPCServer(), adminHandler)
	go grpcSrv.Start()
	defer grpcSrv.Stop()

	// Wait for gRPC to be ready.
	waitForPort("127.0.0.1", grpcPortResolved, 10*time.Second)
	logger.Info("gRPC server ready", zap.Int("port", grpcPortResolved))

	// Register table.
	registerTable(grpcPortResolved, *table, *mode, kafkaBootstrap)
	logger.Info("table registered", zap.String("table", *table))

	// Wait for coordinator to claim table.
	waitForTableClaimed(db, *table, 30*time.Second)
	logger.Info("table claimed", zap.String("table", *table))

	// Wait for Kafka source to be claimed (if Kafka mode).
	if isKafka {
		waitForKafkaSourceClaimed(db, *table, "benchmark", 60*time.Second)
		logger.Info("kafka source claimed", zap.String("table", *table))
	}

	// Run the appropriate benchmark.
	switch *mode {
	case "grpc":
		accepted := runGRPC("127.0.0.1", grpcPortResolved, *table, *records, *apps, *concurrency, *blocking)
		if !waitForDBCount(db, *table, accepted, time.Duration(*timeout)*time.Second) {
			logger.Fatal("DB did not converge after gRPC benchmark")
		}
	case "kafka-proto", "kafka-json":
		// Records are already in Kafka. The coordinator just started consuming.
		// Measure how long until all records land in the DB.
		drainStart := time.Now()
		if !waitForDBCount(db, *table, *records, time.Duration(*timeout)*time.Second) {
			logger.Fatal("DB did not converge — throughput results invalid")
		}
		drainDuration := time.Since(drainStart)

		format := "protobuf"
		if *mode == "kafka-json" {
			format = "JSON"
		}
		printHeader(fmt.Sprintf("KAFKA INGESTION RESULTS (%s)", format))
		fmt.Printf("  Records     : %d\n", *records)
		fmt.Printf("  Partitions  : %d\n", *partitions)
		fmt.Println("  ----------------------------------------")
		fmt.Printf("  Consumer→DB : %d ms  (%.1f s)\n", drainDuration.Milliseconds(), drainDuration.Seconds())
		fmt.Printf("  Throughput  : %.0f rec/s\n", float64(*records)/drainDuration.Seconds())
		printFooter()
	default:
		logger.Fatal("unknown mode", zap.String("mode", *mode))
	}
}

func registerTable(grpcPort int, table string, mode, kafkaBootstrap string) {
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("connect to admin", zap.Error(err))
	}
	defer conn.Close()

	client := coordinatorpb.NewAdminServiceClient(conn)

	// Register table with consolidation/retention disabled for benchmarks.
	cfgMap := map[string]any{
		"consolidation": map[string]any{"enabled": false},
		"retention":     map[string]any{"enabled": false},
	}
	cfgJSON, err := json.Marshal(cfgMap)
	if err != nil {
		logger.Fatal("marshal table config", zap.Error(err))
	}
	cfgStr := string(cfgJSON)

	_, err = client.RegisterTable(context.Background(), &coordinatorpb.RegisterTableRequest{
		TableName:  table,
		ConfigJson: &cfgStr,
	})
	if err != nil {
		logger.Fatal("register table", zap.Error(err))
	}

	// Register Kafka source separately (if Kafka mode).
	if mode == "kafka-proto" || mode == "kafka-json" {
		transformer := "proto"
		if mode == "kafka-json" {
			transformer = "auto"
		}
		_, err = client.RegisterKafkaSource(context.Background(), &coordinatorpb.RegisterKafkaSourceRequest{
			TableName:         table,
			SourceName:          "benchmark",
			Topic:             table,
			BootstrapServers:  kafkaBootstrap,
			RecordTransformer: transformer,
			ConsumerGroupId:   "clp-benchmark-" + table,
		})
		if err != nil {
			logger.Fatal("register kafka source", zap.Error(err))
		}
	}
}

func waitForPort(host string, port int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	logger.Fatal("port not ready", zap.Int("port", port), zap.Duration("timeout", timeout))
}

func waitForTableClaimed(db *sql.DB, table string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var nodeID sql.NullString
		err := db.QueryRow("SELECT node_id FROM _table_assignment WHERE table_name = ?", table).Scan(&nodeID)
		if err == nil && nodeID.Valid && nodeID.String != "" {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	logger.Fatal("table not claimed", zap.String("table", table), zap.Duration("timeout", timeout))
}

func waitForKafkaSourceClaimed(db *sql.DB, table, sourceName string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var nodeID sql.NullString
		err := db.QueryRow("SELECT node_id FROM _kafka_assignment WHERE table_name = ? AND source_name = ?", table, sourceName).Scan(&nodeID)
		if err == nil && nodeID.Valid && nodeID.String != "" {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	logger.Fatal("kafka source not claimed", zap.String("table", table), zap.String("source", sourceName), zap.Duration("timeout", timeout))
}

func waitForDBCount(db *sql.DB, table string, target int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var count int
		if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)).Scan(&count); err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if count >= target {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	var count int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)).Scan(&count); err != nil {
		logger.Error("final count query failed", zap.Error(err))
	}
	logger.Warn("timeout waiting for DB convergence", zap.Int("got", count), zap.Int("want", target))
	return false
}

func writeSchemaTempFile() string {
	f, err := os.CreateTemp("", "schema-*.sql")
	if err != nil {
		logger.Fatal("create temp schema file", zap.Error(err))
	}
	if _, err := f.WriteString(schema.SchemaSQL); err != nil {
		f.Close()
		logger.Fatal("write schema file", zap.Error(err))
	}
	f.Close()
	return f.Name()
}

// ---------------------------------------------------------------------------
// gRPC mode
// ---------------------------------------------------------------------------

func runGRPC(host string, port int, table string, records, apps, concurrency int, blocking bool) int {
	target := fmt.Sprintf("%s:%d", host, port)
	logger.Info("connecting to gRPC", zap.String("target", target))

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("failed to connect", zap.Error(err))
	}
	defer conn.Close()

	client := pb.NewMetadataIngestionServiceClient(conn)

	var (
		accepted atomic.Int64
		rejected atomic.Int64
		wg       sync.WaitGroup
		work     = make(chan int, concurrency)
	)

	logger.Info("sending records via gRPC",
		zap.Int("records", records), zap.Int("apps", apps),
		zap.Int("concurrency", concurrency), zap.String("table", table))

	start := time.Now()

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range work {
				req := buildIngestRequest(table, idx, apps)
				backoff := time.Millisecond
				for {
					resp, err := client.Ingest(context.Background(), req)
					if err != nil {
						if status.Code(err) == codes.ResourceExhausted {
							time.Sleep(backoff)
							backoff = min(backoff*2, 100*time.Millisecond)
							continue
						}
						rejected.Add(1)
						break
					}
					if resp.Accepted {
						accepted.Add(1)
					} else {
						rejected.Add(1)
					}
					break
				}
			}
		}()
	}

	for i := range records {
		work <- i
	}
	close(work)

	wg.Wait()
	elapsed := time.Since(start)

	acc := accepted.Load()
	rej := rejected.Load()

	printHeader("GRPC INGESTION BENCHMARK RESULTS")
	fmt.Printf("  Records     : %d sent, %d accepted, %d rejected\n", records, acc, rej)
	fmt.Printf("  Apps        : %d distinct\n", apps)
	fmt.Printf("  Concurrency : %d max in-flight\n", concurrency)
	fmt.Printf("  Blocking    : %v\n", blocking)
	fmt.Println("  ----------------------------------------")
	fmt.Printf("  Duration    : %d ms  (%.1f s)\n", elapsed.Milliseconds(), elapsed.Seconds())
	fmt.Printf("  Throughput  : %.0f rec/s\n", float64(acc)/elapsed.Seconds())
	printFooter()
	return int(acc)
}

func buildIngestRequest(table string, i, appCount int) *pb.IngestRequest {
	appID := i % appCount
	hourOffset := int64(i % 24)
	recordCount := int32(50000 + (i % 50000))

	service := fmt.Sprintf("app-%03d", appID)
	host := fmt.Sprintf("host-%03d.app-%03d.us-east-1a", appID, appID)
	zone := fmt.Sprintf("us-east-1%c", 'a'+byte(appID%4))
	irPath := fmt.Sprintf("s3://ir-bucket/%s/ir-%09d.clp.zst", service, i)

	const (
		baseTimestamp = 1738368000_000_000_000
		hour          = 3_600_000_000_000
	)
	minTs := baseTimestamp + hourOffset*hour
	maxTs := minTs + hour - 1

	return &pb.IngestRequest{
		TableName: table,
		Record: &pb.MetadataRecord{
			File: &pb.FileFields{
				State:        "IR_CLOSED",
				MinTimestamp: minTs,
				MaxTimestamp: maxTs,
				RecordCount:  recordCount,
				Ir: &pb.IrFileInfo{
					ClpIrStorageBackend: "s3",
					ClpIrBucket:         "ir-bucket",
					ClpIrPath:           irPath,
				},
			},
			Dim: []*pb.DimEntry{
				{Key: "service", Value: &pb.DimensionValue{Value: &pb.DimensionValue_Str{Str: &pb.StringDimension{Value: service, MaxLength: 128}}}},
				{Key: "host", Value: &pb.DimensionValue{Value: &pb.DimensionValue_Str{Str: &pb.StringDimension{Value: host, MaxLength: 128}}}},
				{Key: "zone", Value: &pb.DimensionValue{Value: &pb.DimensionValue_Str{Str: &pb.StringDimension{Value: zone, MaxLength: 128}}}},
			},
			Agg: []*pb.IngestAggEntry{
				{Field: "level", Qualifier: "info", AggType: pb.IngestAggType_GTE, Value: &pb.IngestAggEntry_IntVal{IntVal: int64(recordCount)}},
				{Field: "level", Qualifier: "warn", AggType: pb.IngestAggType_GTE, Value: &pb.IngestAggEntry_IntVal{IntVal: int64(float64(recordCount) * 0.05)}},
				{Field: "level", Qualifier: "error", AggType: pb.IngestAggType_GTE, Value: &pb.IngestAggEntry_IntVal{IntVal: int64(float64(recordCount) * 0.02)}},
				{Field: "level", Qualifier: "fatal", AggType: pb.IngestAggType_GTE, Value: &pb.IngestAggEntry_IntVal{IntVal: int64(float64(recordCount) * 0.002)}},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Kafka mode (protobuf or JSON)
// ---------------------------------------------------------------------------

// produceToKafka produces records to Kafka and prints producer throughput stats.
func produceToKafka(bootstrapServers, topic string, table string, records, apps, batchSize int, useJSON bool) {
	format := "protobuf"
	if useJSON {
		format = "JSON"
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapServers),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxBufferedRecords(batchSize),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		logger.Fatal("create Kafka producer", zap.Error(err))
	}
	defer client.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	logger.Info("producing records", zap.Int("records", records), zap.String("format", format), zap.String("topic", topic))
	produceStart := time.Now()

	// Build all records and produce synchronously in batches.
	var wg sync.WaitGroup
	var produceErr atomic.Value

	for i := 0; i < records; i++ {
		appID := fmt.Sprintf("app-%03d", i%apps)
		record := buildMetadataRecord(rng, appID, i)

		var data []byte
		if useJSON {
			data, err = marshalRecordJSON(record)
		} else {
			data, err = proto.Marshal(record)
		}
		if err != nil {
			logger.Fatal("marshal record", zap.Int("index", i), zap.Error(err))
		}

		wg.Add(1)
		client.Produce(context.Background(), &kgo.Record{
			Topic: topic,
			Key:   []byte(appID),
			Value: data,
		}, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				produceErr.Store(err)
			}
		})
	}

	wg.Wait()
	if e := produceErr.Load(); e != nil {
		logger.Fatal("produce error", zap.Error(e.(error)))
	}

	produceDuration := time.Since(produceStart)

	logger.Info("producer complete",
		zap.Int("records", records), zap.String("format", format),
		zap.Int64("durationMs", produceDuration.Milliseconds()),
		zap.Float64("throughput", float64(records)/produceDuration.Seconds()))
}

func buildMetadataRecord(rng *rand.Rand, appID string, seq int) *pb.MetadataRecord {
	now := time.Now().UnixMilli()
	minTs := now - int64(rng.Intn(3600_000))
	maxTs := minTs + int64(rng.Intn(60_000)) + 1000

	return &pb.MetadataRecord{
		File: &pb.FileFields{
			State:        "IR_CLOSED",
			MinTimestamp: minTs,
			MaxTimestamp: maxTs,
			RawSizeBytes: int64(rng.Intn(10_000_000)) + 100_000,
			RecordCount:  int32(rng.Intn(50_000)) + 1000,
			Ir: &pb.IrFileInfo{
				ClpIrStorageBackend: "s3",
				ClpIrBucket:         "benchmark-bucket",
				ClpIrPath:           fmt.Sprintf("/logs/%s/%d.clp.zst", appID, seq),
				ClpIrSizeBytes:      int64(rng.Intn(5_000_000)) + 50_000,
			},
		},
		Dim: []*pb.DimEntry{
			{Key: "service", Value: &pb.DimensionValue{Value: &pb.DimensionValue_Str{Str: &pb.StringDimension{Value: appID, MaxLength: 64}}}},
			{Key: "host", Value: &pb.DimensionValue{Value: &pb.DimensionValue_Str{Str: &pb.StringDimension{Value: fmt.Sprintf("host-%03d", rng.Intn(50)), MaxLength: 64}}}},
			{Key: "zone", Value: &pb.DimensionValue{Value: &pb.DimensionValue_Str{Str: &pb.StringDimension{Value: zones[rng.Intn(len(zones))], MaxLength: 32}}}},
		},
		Agg: []*pb.IngestAggEntry{
			{Field: "level", Qualifier: "info", AggType: pb.IngestAggType_GTE, Value: &pb.IngestAggEntry_IntVal{IntVal: int64(rng.Intn(40_000))}},
			{Field: "level", Qualifier: "warn", AggType: pb.IngestAggType_GTE, Value: &pb.IngestAggEntry_IntVal{IntVal: int64(rng.Intn(5_000))}},
			{Field: "level", Qualifier: "error", AggType: pb.IngestAggType_GTE, Value: &pb.IngestAggEntry_IntVal{IntVal: int64(rng.Intn(500))}},
			{Field: "level", Qualifier: "fatal", AggType: pb.IngestAggType_GTE, Value: &pb.IngestAggEntry_IntVal{IntVal: int64(rng.Intn(10))}},
		},
	}
}

type jsonRecord struct {
	State        string    `json:"state"`
	MinTimestamp int64     `json:"min_timestamp"`
	MaxTimestamp int64     `json:"max_timestamp"`
	RawSizeBytes int64     `json:"raw_size_bytes"`
	RecordCount  int32     `json:"record_count"`
	IR           *jsonIR   `json:"ir,omitempty"`
	Dims         []jsonDim `json:"dims,omitempty"`
	Aggs         []jsonAgg `json:"aggs,omitempty"`
}

type jsonIR struct {
	StorageBackend string `json:"storage_backend"`
	Bucket         string `json:"bucket"`
	Path           string `json:"path"`
	SizeBytes      int64  `json:"size_bytes"`
}

type jsonDim struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Width int32  `json:"width,omitempty"`
}

type jsonAgg struct {
	Field     string `json:"field"`
	Qualifier string `json:"qualifier"`
	Type      string `json:"type"`
	IntVal    int64  `json:"int_val"`
}

func marshalRecordJSON(record *pb.MetadataRecord) ([]byte, error) {
	f := record.File
	rec := jsonRecord{
		State:        f.State,
		MinTimestamp: f.MinTimestamp,
		MaxTimestamp: f.MaxTimestamp,
		RawSizeBytes: f.RawSizeBytes,
		RecordCount:  f.RecordCount,
	}

	if f.Ir != nil {
		rec.IR = &jsonIR{
			StorageBackend: f.Ir.ClpIrStorageBackend,
			Bucket:         f.Ir.ClpIrBucket,
			Path:           f.Ir.ClpIrPath,
			SizeBytes:      f.Ir.ClpIrSizeBytes,
		}
	}

	for _, d := range record.Dim {
		if str := d.Value.GetStr(); str != nil {
			rec.Dims = append(rec.Dims, jsonDim{Key: d.Key, Value: str.Value, Width: str.MaxLength})
		}
	}

	for _, a := range record.Agg {
		if iv, ok := a.Value.(*pb.IngestAggEntry_IntVal); ok {
			rec.Aggs = append(rec.Aggs, jsonAgg{
				Field: a.Field, Qualifier: a.Qualifier,
				Type: a.AggType.String(), IntVal: iv.IntVal,
			})
		}
	}

	return json.Marshal(rec)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func printHeader(title string) {
	fmt.Println()
	fmt.Println("==========================================")
	fmt.Printf("  %s\n", title)
	fmt.Println("==========================================")
	fmt.Println()
}

func printFooter() {
	fmt.Println("==========================================")
}
