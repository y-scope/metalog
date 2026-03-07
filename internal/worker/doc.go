// Package worker implements the task execution layer for metalog.
//
// The [Prefetcher] batch-claims tasks from the database queue using
// SELECT ... FOR UPDATE SKIP LOCKED and feeds them into a buffered channel.
// Multiple [Core] goroutines consume tasks from the channel, execute archive
// creation via the [Archiver] interface, and report results back through
// the [TaskCompleter].
package worker
