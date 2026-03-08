// Package run provides the top-level entry points for the metalog binary's
// subcommands.
//
// [Server] implements the "serve" subcommand: it runs a config-driven node
// that enables coordinator, worker, and gRPC services based on the YAML
// configuration. It handles signal-based graceful shutdown.
//
// The "admin" subcommand group provides operational utilities such as
// "register-table" for upserting table definitions in the database.
package run
