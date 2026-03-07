// Package taskqueue implements a database-backed task queue using MySQL/MariaDB
// with pessimistic locking (SELECT ... FOR UPDATE SKIP LOCKED).
//
// The [Queue] type provides create, claim, complete, fail, and reclaim
// operations. Task payloads are serialized with MessagePack and compressed
// with LZ4 (see [EncodePayload] and [DecodePayload]).
//
// Workers claim tasks in batches via FOR UPDATE SKIP LOCKED, which avoids
// lock contention between concurrent consumers. Failed tasks are automatically
// reclaimable after a configurable timeout.
package taskqueue
