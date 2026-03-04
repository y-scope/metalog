# Design Documents

[← Back to docs](../README.md)

Design documents capture the "why" behind architectural decisions — edge cases, alternatives considered, walkthroughs, and deep implementation details.

---

## Index

| Document | Summary |
|----------|---------|
| [Coordinator HA](coordinator-ha.md) | Heartbeat vs lease liveness, edge cases (split-brain, race conditions, clock skew), data model, walkthroughs |
| [Early Termination](early-termination.md) | TopN watermark algorithm details, runnable Java example, Presto integration, streaming cursors |
| [Keyset Pagination](keyset-pagination.md) | PK-based keyset cursor design for streaming split queries |

---

## Conventions

- Each document focuses on a single architectural decision or subsystem
- Include alternatives considered and why they were rejected
- Include edge cases and failure modes
- Link back to the corresponding concept or guide document
