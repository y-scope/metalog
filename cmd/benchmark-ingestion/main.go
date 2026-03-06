// Package main implements a unified ingestion benchmark for the metalog coordinator.
// It supports three modes:
//   - grpc:        Send records via MetadataIngestionService.Ingest RPC
//   - kafka-proto: Produce protobuf-encoded records to Kafka
//   - kafka-json:  Produce JSON-encoded records to Kafka
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
)

var zones = []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}

func main() {
	mode := flag.String("mode", "grpc", "Benchmark mode: grpc, kafka-proto, kafka-json")
	records := flag.Int("records", 100000, "Number of records to send")
	apps := flag.Int("apps", 10000, "Number of distinct app IDs")
	table := flag.String("table", "clp_spark", "Target table name")

	// gRPC flags
	grpcHost := flag.String("grpc-host", "localhost", "gRPC server hostname")
	grpcPort := flag.Int("grpc-port", 9091, "gRPC server port")
	concurrency := flag.Int("concurrency", 5000, "Max concurrent in-flight RPCs (grpc mode)")

	// Kafka flags
	bootstrapServers := flag.String("bootstrap-servers", "localhost:9092", "Kafka bootstrap servers")
	topic := flag.String("topic", "clp_spark", "Kafka topic")
	batchSize := flag.Int("batch-size", 1000, "Kafka producer batch size")

	// DB monitor flags
	monitor := flag.Bool("monitor", false, "Monitor MariaDB for end-to-end throughput")
	dbHost := flag.String("db-host", "localhost", "MariaDB host")
	dbPort := flag.Int("db-port", 3306, "MariaDB port")
	dbName := flag.String("db-name", "metalog_metastore", "MariaDB database name")
	dbUser := flag.String("db-user", "root", "MariaDB user")
	dbPassword := flag.String("db-password", "password", "MariaDB password")
	timeout := flag.Int("timeout", 120, "Monitor timeout in seconds")

	flag.Parse()

	switch *mode {
	case "grpc":
		runGRPC(*grpcHost, *grpcPort, *table, *records, *apps, *concurrency)
	case "kafka-proto":
		runKafka(*bootstrapServers, *topic, *table, *records, *apps, *batchSize, false,
			*monitor, *dbHost, *dbPort, *dbName, *dbUser, *dbPassword, *timeout)
	case "kafka-json":
		runKafka(*bootstrapServers, *topic, *table, *records, *apps, *batchSize, true,
			*monitor, *dbHost, *dbPort, *dbName, *dbUser, *dbPassword, *timeout)
	default:
		log.Fatalf("Unknown mode %q; must be grpc, kafka-proto, or kafka-json", *mode)
	}
}

// ---------------------------------------------------------------------------
// gRPC mode
// ---------------------------------------------------------------------------

func runGRPC(host string, port int, table string, records, apps, concurrency int) {
	target := fmt.Sprintf("%s:%d", host, port)
	log.Printf("Connecting to %s ...", target)

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewMetadataIngestionServiceClient(conn)

	var (
		accepted atomic.Int64
		rejected atomic.Int64
		wg       sync.WaitGroup
		sem      = make(chan struct{}, concurrency)
	)

	fmt.Printf("Sending %d records via gRPC (apps=%d, concurrency=%d) to table %q ...\n",
		records, apps, concurrency, table)

	start := time.Now()

	for i := range records {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			req := buildIngestRequest(table, idx, apps)
			resp, err := client.Ingest(context.Background(), req)
			if err != nil {
				rejected.Add(1)
				return
			}
			if resp.Accepted {
				accepted.Add(1)
			} else {
				rejected.Add(1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	acc := accepted.Load()
	rej := rejected.Load()

	printHeader("GRPC INGESTION BENCHMARK RESULTS")
	fmt.Printf("  Records     : %d sent, %d accepted, %d rejected\n", records, acc, rej)
	fmt.Printf("  Apps        : %d distinct\n", apps)
	fmt.Printf("  Concurrency : %d max in-flight\n", concurrency)
	fmt.Println("  ----------------------------------------")
	fmt.Printf("  Duration    : %d ms  (%.1f s)\n", elapsed.Milliseconds(), elapsed.Seconds())
	fmt.Printf("  Throughput  : %.0f rec/s\n", float64(records)/elapsed.Seconds())
	printFooter()
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
			Agg: []*pb.AggEntry{
				{Field: "level", Qualifier: "info", AggType: pb.AggType_GTE, Value: &pb.AggEntry_IntVal{IntVal: int64(recordCount)}},
				{Field: "level", Qualifier: "warn", AggType: pb.AggType_GTE, Value: &pb.AggEntry_IntVal{IntVal: int64(float64(recordCount) * 0.05)}},
				{Field: "level", Qualifier: "error", AggType: pb.AggType_GTE, Value: &pb.AggEntry_IntVal{IntVal: int64(float64(recordCount) * 0.02)}},
				{Field: "level", Qualifier: "fatal", AggType: pb.AggType_GTE, Value: &pb.AggEntry_IntVal{IntVal: int64(float64(recordCount) * 0.002)}},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Kafka mode (protobuf or JSON)
// ---------------------------------------------------------------------------

func runKafka(bootstrapServers, topic, table string, records, apps, batchSize int, useJSON bool,
	monitor bool, dbHost string, dbPort int, dbName, dbUser, dbPassword string, timeout int) {

	format := "protobuf"
	if useJSON {
		format = "JSON"
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"batch.size":        batchSize,
		"linger.ms":         5,
		"acks":              "all",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Drain delivery reports in background.
	delivered := make(chan int, 1)
	failed := make(chan error, 1)
	go func() {
		count := 0
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					failed <- fmt.Errorf("delivery failed: %v", ev.TopicPartition.Error)
					return
				}
				count++
				if count == records {
					delivered <- count
					return
				}
			}
		}
	}()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	fmt.Printf("Producing %d records (%s) to topic %q ...\n", records, format, topic)
	e2eStart := time.Now()
	produceStart := e2eStart

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
			log.Fatalf("Failed to marshal record %d: %v", i, err)
		}

		topicStr := topic
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicStr, Partition: kafka.PartitionAny},
			Key:            []byte(appID),
			Value:          data,
		}
		for {
			err = producer.Produce(msg, nil)
			if err == nil {
				break
			}
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				producer.Flush(500)
				continue
			}
			log.Fatalf("Failed to produce record %d: %v", i, err)
		}
	}

	// Wait for all deliveries.
	producer.Flush(30 * 1000)
	select {
	case <-delivered:
	case err := <-failed:
		log.Fatalf("Produce error: %v", err)
	case <-time.After(60 * time.Second):
		log.Fatal("Timed out waiting for delivery confirmations")
	}

	produceDuration := time.Since(produceStart)

	printHeader(fmt.Sprintf("KAFKA INGESTION BENCHMARK RESULTS (%s)", format))
	fmt.Printf("  Records     : %d\n", records)
	fmt.Printf("  Apps        : %d distinct\n", apps)
	fmt.Printf("  Format      : %s\n", format)
	fmt.Println("  ----------------------------------------")
	fmt.Println("  Producer")
	fmt.Printf("    Duration  : %d ms\n", produceDuration.Milliseconds())
	fmt.Printf("    Throughput: %d rec/s\n", int(float64(records)/produceDuration.Seconds()))

	// Optionally monitor DB for end-to-end throughput.
	if monitor {
		fmt.Println("  ----------------------------------------")
		fmt.Println("  End-to-end (Kafka -> DB)")

		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", dbUser, dbPassword, dbHost, dbPort, dbName)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Fatalf("Failed to connect to MariaDB: %v", err)
		}
		defer db.Close()

		query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
		deadline := e2eStart.Add(time.Duration(timeout) * time.Second)

		for {
			var count int
			if err := db.QueryRow(query).Scan(&count); err != nil {
				log.Fatalf("DB query failed: %v", err)
			}
			if count >= records {
				e2eDuration := time.Since(e2eStart)
				fmt.Printf("    Duration  : %d ms  (%.1f s)\n", e2eDuration.Milliseconds(), e2eDuration.Seconds())
				fmt.Printf("    Throughput: %d rec/s\n", int(float64(records)/e2eDuration.Seconds()))
				break
			}
			if time.Now().After(deadline) {
				fmt.Fprintf(os.Stderr, "    TIMEOUT: only %d/%d records found after %ds\n", count, records, timeout)
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	printFooter()
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
		Agg: []*pb.AggEntry{
			{Field: "level", Qualifier: "info", AggType: pb.AggType_GTE, Value: &pb.AggEntry_IntVal{IntVal: int64(rng.Intn(40_000))}},
			{Field: "level", Qualifier: "warn", AggType: pb.AggType_GTE, Value: &pb.AggEntry_IntVal{IntVal: int64(rng.Intn(5_000))}},
			{Field: "level", Qualifier: "error", AggType: pb.AggType_GTE, Value: &pb.AggEntry_IntVal{IntVal: int64(rng.Intn(500))}},
			{Field: "level", Qualifier: "fatal", AggType: pb.AggType_GTE, Value: &pb.AggEntry_IntVal{IntVal: int64(rng.Intn(10))}},
		},
	}
}

// jsonRecord matches the format expected by the consumer's unmarshalJSONToProto.
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
		if iv, ok := a.Value.(*pb.AggEntry_IntVal); ok {
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
