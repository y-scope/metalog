// Package registry handles table assignment, node liveness, and HA
// coordination for coordinator nodes. It provides the database operations
// for claiming tables, sending heartbeats or renewing leases, detecting
// dead nodes, and reclaiming orphaned table assignments.
package registry
