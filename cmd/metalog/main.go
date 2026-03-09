package main

import (
	"fmt"
	"os"

	"github.com/y-scope/metalog/run"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		os.Args = append(os.Args[:1], os.Args[2:]...)
		run.Server()
	case "admin":
		run.Admin(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: metalog <command> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  serve    Start the metalog node (coordinator, workers, gRPC)")
	fmt.Fprintln(os.Stderr, "  admin    Administrative operations (register-table, etc.)")
}
