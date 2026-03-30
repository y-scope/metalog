package main

import (
	"context"
	"fmt"
	"io"
	"time"

	metapb "github.com/y-scope/metalog/gen/proto/metadatapb"
	splitspb "github.com/y-scope/metalog/gen/proto/splitspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Add RPC-Service header required by the proxy
	ctx = metadata.AppendToOutgoingContext(ctx, "rpc-service", "metalog")

	conn, err := grpc.NewClient("localhost:5435", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println("dial error:", err)
		return
	}
	defer conn.Close()

	// 1. List tables
	mc := metapb.NewMetadataServiceClient(conn)
	tables, err := mc.ListTables(ctx, &metapb.ListTablesRequest{})
	if err != nil {
		fmt.Println("ListTables error:", err)
		return
	}
	fmt.Println("Tables:", tables.Tables)

	if len(tables.Tables) == 0 {
		fmt.Println("No tables found, skipping StreamSplits")
		return
	}

	// 2. StreamSplits on first table
	table := tables.Tables[0]
	fmt.Printf("\nStreaming splits for table %q (limit 3)...\n\n", table)

	qc := splitspb.NewSplitQueryServiceClient(conn)
	stream, err := qc.StreamSplits(ctx, &splitspb.StreamSplitsRequest{
		Table: table,
		OrderBy: []*splitspb.OrderBy{{
			Column: "__FILE.min_timestamp",
			Order:  splitspb.Order_ORDER_DESC,
		}},
		Limit: 3,
	})
	if err != nil {
		fmt.Println("StreamSplits error:", err)
		return
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("recv error:", err)
			return
		}
		if resp.Done {
			fmt.Printf("--- Done. Scanned=%d Matched=%d Truncated=%v\n",
				resp.Stats.SplitsScanned, resp.Stats.SplitsMatched, resp.Stats.Truncated)
			break
		}
		s := resp.Split
		fmt.Printf("[%d] state=%s records=%d time=[%d, %d] dims=%v\n",
			resp.Sequence, s.File.State, s.File.RecordCount,
			s.File.MinTimestamp, s.File.MaxTimestamp, s.Dimensions)
	}
}
