# Task Queue Functional Tests

Correctness tests for the database-backed task queue. Verifies the full state machine,
concurrent claiming, stale task reclaim, dead-letter promotion, graceful shutdown reset,
and the `TaskPrefetcher` lifecycle.

These tests use [Testcontainers](https://testcontainers.com/) and spin up a MariaDB
instance automatically — no separate Docker Compose setup is required.

## What it covers

| Test class | Scenarios |
|------------|-----------|
| `TaskQueueTest` | pending→processing→completed/failed, stale reclaim, dead-letter, cleanup, concurrent claims |
| `TaskPrefetcherTest` | queue fill/drain, backoff behaviour, graceful shutdown reset, concurrent workers |

## Prerequisites

- Docker (required by Testcontainers)
- JDK 17+
- Maven 3.9+

## Usage

```bash
# From the clp-service directory
./integration-tests/functional/task-queue/run.py
```

Or run directly with Maven:

```bash
mvn test -Dtest="TaskQueueTest,TaskPrefetcherTest"
```

## Port customization

Testcontainers assigns a random host port for each run — no `.env` configuration needed.
