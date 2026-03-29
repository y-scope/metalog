package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
)

// Admin dispatches admin subcommands.
func runAdmin(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: metalog admin <command>")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "commands:")
		fmt.Fprintln(os.Stderr, "  register-table   Register a new table via the admin gRPC service")
		os.Exit(1)
	}

	switch args[0] {
	case "register-table":
		adminRegisterTable(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown admin command: %s\n", args[0])
		os.Exit(1)
	}
}

func adminRegisterTable(args []string) {
	fs := flag.NewFlagSet("metalog admin register-table", flag.ExitOnError)
	addr := fs.String("addr", "localhost:9090", "gRPC server address")
	tableName := fs.String("table", "", "table name (required)")
	displayName := fs.String("display-name", "", "human-readable display name")
	configJSON := fs.String("config-json", "", `JSON config blob merged into stored config (e.g. '{"consolidation":{"enabled":true},"retention":{"enabled":true}}')`)
	_ = fs.Parse(args)

	if *tableName == "" {
		fmt.Fprintln(os.Stderr, "error: --table is required")
		fs.Usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close() //nolint:errcheck // cleanup

	client := pb.NewAdminServiceClient(conn)

	req := &pb.RegisterTableRequest{
		TableName:   *tableName,
		DisplayName: *displayName,
	}
	if *configJSON != "" {
		req.ConfigJson = configJSON
	}

	resp, err := client.RegisterTable(ctx, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "register-table failed: %v\n", err)
		os.Exit(1)
	}

	if resp.Created {
		fmt.Printf("table %q created\n", resp.TableName)
	} else {
		fmt.Printf("table %q updated (already existed)\n", resp.TableName)
	}
}

