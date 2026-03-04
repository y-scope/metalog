# Quickstart

[← Back to docs](../README.md)

Setup and local development guide for the CLP Metastore Service.

## Prerequisites

- Docker and Docker Compose
- JDK 17+
- Maven 3.9+

## Quick Start

### 1. Start Infrastructure

```bash
./docker/start.sh -d
```

This builds the CLP core package if needed, then starts all services (MariaDB, Kafka, MinIO, coordinator nodes).

Verify services are healthy:

```bash
docker compose -f docker/docker-compose.yml ps
```

All services should report healthy status.

### 2. Build the Service

```bash
mvn clean package -DskipTests
```

Produces `grpc-server/target/metalog-grpc-server-1.0-SNAPSHOT.jar` with all dependencies.

### 3. Run the Node (Coordinator + Workers)

`Node` is the main entry point. It hosts coordinators and workers in a single JVM.

```bash
# Run with default config path (/etc/clp/node.yaml)
java -jar grpc-server/target/metalog-grpc-server-1.0-SNAPSHOT.jar

# Or specify a config file
java -jar grpc-server/target/metalog-grpc-server-1.0-SNAPSHOT.jar config/node.yaml
```

Expected output:

```
INFO  ServerMain - Starting server with config: /etc/clp/node.yaml
INFO  Node - Created shared HikariDataSource: poolSize=20, poolMinIdle=5
INFO  Node - Created shared StorageRegistry: defaultBackend=minio, backends=[minio], clpBinaryPath=/usr/bin/clp-s
INFO  Node - Starting unit: coordinator-1 (type=coordinator)
INFO  Node - Starting unit: worker-1 (type=worker)
INFO  Node - Node started with 2 units
```

### 4. Run the API Server (Optional)

The API server provides gRPC access to metadata.

```bash
java -cp grpc-server/target/metalog-grpc-server-1.0-SNAPSHOT.jar \
  com.yscope.metalog.query.api.server.vertx.VertxApiServer
```

Expected output:

```
INFO  GrpcServer - gRPC server started on port 9090
```

---

## Running with Docker Compose

Use `docker/start.sh` — it ensures the CLP binary package is built before starting.

```bash
# Start everything (detached)
./docker/start.sh -d

# Force a fresh CLP build, then start
./docker/start.sh --rebuild-clp -d

# Scale coordinator nodes
./docker/start.sh -d --scale coordinator-node=3

# View logs
docker compose -f docker/docker-compose.yml logs -f coordinator-node
```

## Running Tests

```bash
# Run all tests
mvn test

# Run a specific test class
mvn test -Dtest=SparkJobPolicyTest

# Run integration tests only
mvn test -Dtest="*IntegrationTest"
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `DB_HOST` | `localhost` | Database host |
| `DB_PORT` | `3306` | Database port |
| `DB_USER` | `root` | Database user |
| `DB_PASSWORD` | `password` | Database password |
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |

### Node Configuration (YAML)

Node-level settings (database, storage, health) are defined in YAML. Per-table configuration lives in the database, but tables can be declared in YAML for automatic registration on startup:

```yaml
node:
  name: coordinator-node-1
  nodeIdEnvVar: HOSTNAME       # env var for _table_assignment.node_id
  database:
    host: localhost
    port: 3306
    database: metalog_metastore
    user: root
    password: password
    poolSize: 20
  storage:
    defaultBackend: minio
    backends:
      minio:
        endpoint: http://localhost:9000
        accessKey: minioadmin
        secretKey: minioadmin
        forcePathStyle: true
  health:
    enabled: true
    port: 8081

# Declarative table registration (auto-UPSERTed on startup).
# Only specified fields are updated; omitted fields keep DB defaults.
tables:
  - name: clp_spark
    displayName: Spark Logs
    kafka:
      topic: spark-ir
      bootstrapServers: localhost:9092

# Shared worker pool (claims tasks from all tables)
worker:
  numWorkers: 4    # 0 = coordinator-only node
```

See [Configuration Reference](../reference/configuration.md) for full details.

## Verification

Confirm the system is operational:

```bash
# Check database tables
docker compose -f docker/docker-compose.yml exec mariadb mariadb -uroot -ppassword metalog_metastore \
  -e "SHOW TABLES;"

# Check Kafka topics
docker compose -f docker/docker-compose.yml exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --list

# Check MinIO buckets
docker compose -f docker/docker-compose.yml exec minio mc ls local/
```

### End-to-End Validation Script

For a thorough automated check — HA fight-for-master, Kafka ingestion, and single-owner enforcement — use the E2E validation script:

```bash
./integration-tests/functional/coordinator/validate-e2e.sh
```

What it tests:
1. Two coordinator nodes start and each gets a unique node ID (via `HOSTNAME`)
2. Unassigned tables are claimed within seconds; no double-claims occur
3. Kafka messages are ingested into the `clp_spark` metadata table
4. Only one coordinator runs per table (verified via logs)
5. Reconciliation: a table added after startup is picked up within seconds

Prerequisites: Docker, a built JAR (`mvn package -DskipTests`), and port `3307` free (or set `DB_PORT`).

## Troubleshooting

**Services not starting:**
```bash
docker compose -f docker/docker-compose.yml logs mariadb kafka minio
```

**Connection refused errors:**
- Ensure infrastructure is healthy: `docker compose -f docker/docker-compose.yml ps`
- Check ports are not in use: `lsof -i :3306 -i :9092 -i :9000`

**Tests failing:**
- Ensure Docker is running (tests use Testcontainers)
- Check for port conflicts with running infrastructure

## See Also

- [Tutorial: End-to-End Ingestion](tutorial-ingestion.md) — Walk through the full Kafka ingestion pipeline
- [Architecture Overview](../concepts/overview.md) — Component design and data flow
- [Configuration Reference](../reference/configuration.md) — Detailed configuration reference
- [Scale Workers](../guides/scale-workers.md) — Scaling workers
