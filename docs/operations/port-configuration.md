# Port Configuration Guide

[← Back to docs](../README.md)

The CLP Metastore Service uses several ports for its infrastructure. All ports are customizable via environment variables.

## Quick Start

### Default Ports (No Configuration Needed)
```bash
integration-tests/benchmarks/kafka-ingestion/run.py
```

Uses default ports:
- MariaDB/MySQL: 3306
- Kafka: 9092
- MinIO API: 9000
- MinIO Console: 9001

### Custom Ports (Avoid Conflicts)

**Option 1: Set environment variables directly**
```bash
export MINIO_PORT=9002
export MINIO_CONSOLE_PORT=9003
integration-tests/benchmarks/kafka-ingestion/run.py
```

**Option 2: Create a .env file from the example**
```bash
cp docker/.env.example docker/.env
# Edit docker/.env with your preferred ports, then:
./docker/start.sh -d
```

## Available Port Variables

### Infrastructure (Docker Compose)

| Variable | Default | Purpose |
|----------|---------|---------|
| `DB_PORT` | 3306 | Database port (used by Docker Compose and the application) |
| `KAFKA_PORT` | 9092 | Kafka broker port |
| `MINIO_PORT` | 9000 | MinIO S3 API port |
| `MINIO_CONSOLE_PORT` | 9001 | MinIO web console port |

### Application

| Port | Purpose | Configured in |
|------|---------|---------------|
| 9090 | Unified gRPC server (ingestion, query, catalog, admin) | `node.yaml` → `node.grpc.port` |
| 8081 | Health check (`/health/live`, `/health/ready`) | `node.yaml` → `node.health.port` |

## Common Port Conflicts

### Port 9000 Conflict
**Cause:** Zscaler, Portainer, SonarQube, MinIO's own console running on an older default

**Solution:**
```bash
export MINIO_PORT=9002
# Or add to .env file:
echo "MINIO_PORT=9002" >> .env
```

### Port 3306 Conflict
**Cause:** Existing MariaDB/MySQL installation

**Solution:**
```bash
export DB_PORT=3307
# Or add to .env file:
echo "DB_PORT=3307" >> .env
```

### Port 9092 Conflict
**Cause:** Existing Kafka installation

**Solution:**
```bash
export KAFKA_PORT=9093
# Or add to .env file
```

## Checking What's Using a Port

```bash
# macOS
lsof -i :9000
netstat -anv | grep "\.9000 "

# Linux
lsof -i :9000
netstat -tulpn | grep :9000
```

## Docker Compose Manual Override

You can also override ports directly when starting services:

```bash
MINIO_PORT=9002 ./docker/start.sh -d
```

## Verifying Configuration

After starting services, verify the ports:

```bash
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

You should see your custom ports in the output.

## See Also

- [Configuration Reference](../reference/configuration.md) — Full node.yaml reference and env overrides
- [Quickstart](../getting-started/quickstart.md) — Setup and first run
