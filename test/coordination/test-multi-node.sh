#!/usr/bin/env bash
#
# Multi-node coordination test for Metalog.
#
# What it tests:
#   1. Fight-for-master: two nodes race, exactly one claims each table
#   2. Kafka JSON ingestion into the metadata table
#   3. Unique nodeIds from HOSTNAME env var
#   4. Periodic reconciliation: table added after startup is picked up
#
# Usage:
#   ./test/coordination/test-multi-node.sh
#
# Prerequisites:
#   - Docker with compose v2
#   - Port 3307 free (or set DB_PORT)
#   - CLP core .deb package (auto-built if missing)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_DIR"

# ---------------------------------------------------------------------------
# Docker compose detection
# ---------------------------------------------------------------------------
if [ -n "${COMPOSE:-}" ]; then
    :
elif docker compose version &>/dev/null; then
    COMPOSE="docker compose"
else
    echo "ERROR: 'docker compose' not found" >&2
    exit 1
fi
COMPOSE="$COMPOSE -f $PROJECT_DIR/docker/docker-compose.yml"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}----${NC} $1"; }

FAILURES=0
export DB_PORT="${DB_PORT:-3307}"

db_exec() {
    docker exec metalog-mariadb mariadb -uroot -ppassword metalog_metastore -sN -e "$1" 2>/dev/null
}

kafka_produce() {
    echo "$2" | tr -d '\n' | tr -s ' ' | \
        docker exec -i metalog-kafka kafka-console-producer \
            --bootstrap-server localhost:9092 --topic "$1"
}

wait_for() {
    local desc="$1" cmd="$2" max="${3:-30}" interval="${4:-2}"
    info "Waiting: $desc"
    for _ in $(seq 1 "$max"); do
        if eval "$cmd" 2>/dev/null; then return 0; fi
        sleep "$interval"
    done
    return 1
}

cleanup() {
    info "Cleaning up..."
    timeout 30 $COMPOSE down -v --timeout 10 2>/dev/null || true
}
trap cleanup EXIT

# Capture compose logs once into a temp file for fast repeated grep.
LOGFILE=$(mktemp)
capture_logs() { $COMPOSE logs coordinator-node >"$LOGFILE" 2>&1; }
trap 'cleanup; rm -f "$LOGFILE"' EXIT

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
info "Cleaning up previous runs..."
timeout 30 $COMPOSE down -v --timeout 10 2>/dev/null || true
docker rm -f metalog-mariadb metalog-kafka metalog-minio \
    metalog-minio-init metalog-log-viewer-setup 2>/dev/null || true
docker ps -a --format '{{.Names}}' | grep '^docker-coordinator-node-' | xargs -r docker rm -f 2>/dev/null || true

# Kill stale containers holding our ports.
for port in "$DB_PORT" 9090 8081 9092 9000 9001; do
    cid=$(docker ps --format '{{.ID}}\t{{.Ports}}' 2>/dev/null | grep -E ":${port}[^0-9]" | awk '{print $1}' || true)
    [ -n "$cid" ] && { info "Removing stale container $cid on port $port"; docker rm -f "$cid" 2>/dev/null || true; }
done
docker network rm docker_metalog-network 2>/dev/null || true

# Build CLP .deb if missing.
if ! ls "$PROJECT_DIR"/clp-core_*.deb >/dev/null 2>&1; then
    info "Building CLP core .deb package..."
    "$PROJECT_DIR/docker/build-clp.sh"
fi

info "Building Docker image..."
$COMPOSE build coordinator-node --quiet 2>&1

info "Starting infrastructure with 2 coordinator replicas..."
DB_PORT="$DB_PORT" $COMPOSE up -d --scale coordinator-node=2 2>&1

wait_for "MariaDB healthy" \
    "docker exec metalog-mariadb mariadb-admin ping -uroot -ppassword 2>/dev/null | grep -q alive" \
    60 2 || { fail "MariaDB did not start"; exit 1; }

wait_for "Kafka healthy" \
    "docker exec metalog-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1" \
    30 2 || { fail "Kafka did not start"; exit 1; }

info "Infrastructure ready"

# ---------------------------------------------------------------------------
# Test 1: Fight-for-master (single claim, single coordinator)
# ---------------------------------------------------------------------------
echo ""
info "=== Test 1: Fight-for-master ==="

db_exec "
INSERT IGNORE INTO _table (table_name, display_name, active) VALUES ('clp_spark', 'Spark Logs', true);
INSERT IGNORE INTO _table_config (table_name, config) VALUES ('clp_spark', '{\"kafka\":{\"enabled\":true,\"topic\":\"clp_spark\",\"bootstrap_servers\":\"kafka:29092\"},\"consolidation\":{\"enabled\":true},\"retention\":{\"enabled\":true,\"type\":\"default\"}}');
INSERT IGNORE INTO _table_assignment (table_name, node_id) VALUES ('clp_spark', NULL);
"

$COMPOSE restart coordinator-node 2>&1

wait_for "clp_spark claimed" \
    "db_exec \"SELECT node_id FROM _table_assignment WHERE table_name = 'clp_spark' AND node_id IS NOT NULL\" | grep -q ." \
    45 3 || { fail "clp_spark not claimed"; capture_logs; tail -30 "$LOGFILE"; exit 1; }

ASSIGNED_NODE=$(db_exec "SELECT node_id FROM _table_assignment WHERE table_name = 'clp_spark'")
if [ -n "$ASSIGNED_NODE" ] && [ "$ASSIGNED_NODE" != "NULL" ]; then
    pass "clp_spark claimed by node '$ASSIGNED_NODE'"
else
    fail "clp_spark not claimed (node_id='$ASSIGNED_NODE')"
fi

capture_logs
CLAIM_COUNT=$(grep -c '"claimed table".*"clp_spark"' "$LOGFILE" || true)
if [ "$CLAIM_COUNT" -eq 1 ]; then
    pass "Exactly 1 claim (no double-claim)"
else
    fail "Expected 1 claim, got $CLAIM_COUNT"
fi

COORD_COUNT=$(grep -c '"starting coordinator unit".*"clp_spark"' "$LOGFILE" || true)
if [ "$COORD_COUNT" -eq 1 ]; then
    pass "Exactly 1 coordinator started"
else
    fail "Expected 1 coordinator, got $COORD_COUNT"
fi

# ---------------------------------------------------------------------------
# Test 2: Kafka ingestion
# ---------------------------------------------------------------------------
echo ""
info "=== Test 2: Kafka metadata ingestion ==="

docker exec metalog-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 --topic clp_spark \
    --partitions 1 --replication-factor 1 2>/dev/null || true

info "Producing 3 test records..."
kafka_produce "clp_spark" '{"ir":{"storage_backend":"s3","bucket":"clp-ir","path":"s3://clp-ir/app-001/executor-0/file-001.clp.zst","size_bytes":1048576},"state":"IR_ARCHIVE_BUFFERING","min_timestamp":1704067200,"max_timestamp":1704067500,"record_count":1000,"raw_size_bytes":5242880,"retention_days":30,"expires_at":0,"self_describing_kv":[{"key":"agg_int/GTE/level/debug","value":"900"},{"key":"agg_int/GTE/level/info","value":"700"},{"key":"agg_int/GTE/level/warn","value":"100"},{"key":"agg_int/GTE/level/error","value":"10"},{"key":"agg_int/GTE/level/fatal","value":"1"}]}'
kafka_produce "clp_spark" '{"ir":{"storage_backend":"s3","bucket":"clp-ir","path":"s3://clp-ir/app-001/executor-0/file-002.clp.zst","size_bytes":2097152},"state":"IR_ARCHIVE_BUFFERING","min_timestamp":1704067200,"max_timestamp":1704067800,"record_count":2500,"raw_size_bytes":10485760,"retention_days":30,"expires_at":0,"self_describing_kv":[{"key":"agg_int/GTE/level/debug","value":"2200"},{"key":"agg_int/GTE/level/info","value":"1800"},{"key":"agg_int/GTE/level/warn","value":"300"},{"key":"agg_int/GTE/level/error","value":"25"},{"key":"agg_int/GTE/level/fatal","value":"0"}]}'
kafka_produce "clp_spark" '{"ir":{"storage_backend":"minio","bucket":"clp-ir","path":"s3://clp-ir/app-002/executor-1/file-003.clp.zst","size_bytes":524288},"state":"IR_ARCHIVE_CONSOLIDATION_PENDING","min_timestamp":1704153600,"max_timestamp":1704154200,"record_count":500,"raw_size_bytes":2621440,"retention_days":7,"expires_at":0,"self_describing_kv":[{"key":"agg_int/GTE/level/debug","value":"480"},{"key":"agg_int/GTE/level/info","value":"400"},{"key":"agg_int/GTE/level/warn","value":"50"},{"key":"agg_int/GTE/level/error","value":"5"},{"key":"agg_int/GTE/level/fatal","value":"0"}]}'

if wait_for "records in DB" \
    "[ \$(db_exec 'SELECT COUNT(*) FROM clp_spark' 2>/dev/null) -ge 3 ]" \
    30 2; then
    ROW_COUNT=$(db_exec "SELECT COUNT(*) FROM clp_spark")
    pass "Ingested $ROW_COUNT records"

    TOTAL_RECORDS=$(db_exec "SELECT SUM(record_count) FROM clp_spark")
    if [ "$TOTAL_RECORDS" -eq 4000 ]; then
        pass "Total record_count = 4000"
    else
        fail "Expected record_count 4000, got $TOTAL_RECORDS"
    fi

    STATES=$(db_exec "SELECT DISTINCT state FROM clp_spark ORDER BY state" | tr '\n' ',')
    if echo "$STATES" | grep -q "IR_ARCHIVE_BUFFERING" && echo "$STATES" | grep -q "IR_ARCHIVE_CONSOLIDATION_PENDING"; then
        pass "Both expected states present"
    else
        fail "Unexpected states: $STATES"
    fi

    PATH_COUNT=$(db_exec "SELECT COUNT(DISTINCT clp_ir_path) FROM clp_spark")
    if [ "$PATH_COUNT" -eq 3 ]; then
        pass "All 3 unique IR paths stored"
    else
        fail "Expected 3 IR paths, got $PATH_COUNT"
    fi
else
    fail "Records did not appear within timeout"
    capture_logs; grep -iE "error|warn" "$LOGFILE" | tail -20
fi

# ---------------------------------------------------------------------------
# Test 3: Unique nodeIds
# ---------------------------------------------------------------------------
echo ""
info "=== Test 3: Unique nodeIds ==="

capture_logs
NODE_IDS=$(grep -oP '"nodeId":"[^"]+' "$LOGFILE" | sed 's/"nodeId":"//' | sort -u || true)
NODE_COUNT=$(echo "$NODE_IDS" | grep -c . || true)

if [ "$NODE_COUNT" -eq 2 ]; then
    pass "2 unique nodeIds"
    echo "$NODE_IDS" | sed 's/^/  /'
else
    fail "Expected 2 nodeIds, got $NODE_COUNT"
fi

# ---------------------------------------------------------------------------
# Test 4: Periodic reconciliation
# ---------------------------------------------------------------------------
echo ""
info "=== Test 4: Periodic reconciliation ==="

db_exec "
INSERT IGNORE INTO _table (table_name, display_name, active) VALUES ('clp_flink', 'Flink Logs', true);
INSERT IGNORE INTO _table_config (table_name, config) VALUES ('clp_flink', '{\"kafka\":{\"enabled\":true,\"topic\":\"clp_flink\",\"bootstrap_servers\":\"kafka:29092\"},\"consolidation\":{\"enabled\":true},\"retention\":{\"enabled\":true,\"type\":\"default\"}}');
INSERT IGNORE INTO _table_assignment (table_name, node_id) VALUES ('clp_flink', NULL);
"

if wait_for "clp_flink claimed" \
    "db_exec \"SELECT node_id FROM _table_assignment WHERE table_name = 'clp_flink' AND node_id IS NOT NULL\" | grep -q ." \
    15 2; then
    FLINK_OWNER=$(db_exec "SELECT node_id FROM _table_assignment WHERE table_name = 'clp_flink'")
    pass "clp_flink claimed by '$FLINK_OWNER' (no restart needed)"
else
    fail "clp_flink not claimed within 30s"
fi

if wait_for "coordinator for clp_flink" \
    "capture_logs && grep -q '\"starting coordinator\".*\"clp_flink\"' \"$LOGFILE\"" \
    10 2; then
    pass "CoordinatorUnit started for clp_flink"
else
    fail "No coordinator started for clp_flink"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "========================================="
if [ "$FAILURES" -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
else
    echo -e "${RED}$FAILURES test(s) failed${NC}"
fi
echo "========================================="
exit "$FAILURES"
