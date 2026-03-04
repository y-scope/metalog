package config

import "time"

// Default timeout and backoff constants.
const (
	// DefaultDeadlockMaxRetries is the maximum number of retries on deadlock.
	DefaultDeadlockMaxRetries = 10

	// DefaultDeadlockMinBackoff is the minimum jitter delay on deadlock retry.
	DefaultDeadlockMinBackoff = 1 * time.Millisecond

	// DefaultDeadlockMaxBackoff is the maximum jitter delay on deadlock retry.
	DefaultDeadlockMaxBackoff = 50 * time.Millisecond

	// DefaultBatchFlushInterval is how often the batching writer flushes.
	DefaultBatchFlushInterval = 1 * time.Second

	// DefaultBatchSize is the default UPSERT batch size.
	DefaultBatchSize = 5000

	// DefaultTaskClaimBatchSize is how many tasks a worker claims at once.
	DefaultTaskClaimBatchSize = 10

	// DefaultTaskStaleTimeout is how long before a processing task is reclaimed.
	DefaultTaskStaleTimeout = 5 * time.Minute

	// DefaultTaskCleanupAge is how old a completed/failed task must be before cleanup.
	DefaultTaskCleanupAge = 24 * time.Hour

	// DefaultWorkerPollInterval is how often workers poll for tasks.
	DefaultWorkerPollInterval = 2 * time.Second

	// DefaultWorkerBackoffMax is the maximum backoff when no tasks are available.
	DefaultWorkerBackoffMax = 30 * time.Second

	// DefaultProgressStallTimeout detects stalled coordinator loops.
	DefaultProgressStallTimeout = 5 * time.Minute

	// DefaultPlannerInterval is how often the consolidation planner runs.
	DefaultPlannerInterval = 60 * time.Second

	// DefaultQueryLimit is the default page size for split queries.
	DefaultQueryLimit = 1000
)
