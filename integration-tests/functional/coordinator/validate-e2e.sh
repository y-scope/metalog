#!/usr/bin/env bash
#
# End-to-end validation script for CLP Service fight-for-master and metadata ingestion.
#
# What it tests:
#   1. Two nodes start with unique nodeIds (from HOSTNAME env var)
#   2. Fight-for-master: unassigned tables are claimed, no double-claims
#   3. Kafka messages are ingested into the clp_spark metadata table
#   4. Only one coordinator runs per table (verified via logs)
#   5. Periodic reconciliation: table added after startup is picked up within seconds
#
# Usage:
#   ./docker/validate-e2e.sh
#
# Prerequisites:
#   - Docker and $COMPOSE
#   - Port 3307 free (database), or set DB_PORT
#   - Built image (script builds if needed)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$PROJECT_DIR"

COMPOSE="$COMPOSE -f $PROJECT_DIR/docker/docker-compose.yml"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}----${NC} $1"; }

FAILURES=0
export DB_PORT="${DB_PORT:-3307}"

db_exec() {
    docker exec clp-mariadb mariadb -uroot -ppassword clp_metastore -sN -e "$1" 2>/dev/null
}

kafka_produce() {
    local topic="$1"
    local message="$2"
    # Compact JSON to single line — kafka-console-producer treats each line as a message
    local compact
    compact=$(echo "$message" | tr -d '\n' | tr -s ' ')
    echo "$compact" | docker exec -i clp-kafka kafka-console-producer \
        --bootstrap-server localhost:9092 \
        --topic "$topic"
}

wait_for_condition() {
    local description="$1"
    local command="$2"
    local max_attempts="${3:-30}"
    local sleep_sec="${4:-2}"

    info "Waiting for: $description (max ${max_attempts}x${sleep_sec}s)"
    for i in $(seq 1 "$max_attempts"); do
        if eval "$command" 2>/dev/null; then
            return 0
        fi
        sleep "$sleep_sec"
    done
    return 1
}

cleanup() {
    info "Cleaning up..."
    $COMPOSE down -v 2>/dev/null || true
}

# =========================================================================
# Setup
# =========================================================================
info "Cleaning up previous runs..."
$COMPOSE down -v 2>/dev/null || true

# Ensure CLP core .deb package is available for Docker image build
# The coordinator-node service requires this package to install CLP binaries
if ! ls "$PROJECT_DIR"/clp-core_*.deb >/dev/null 2>&1; then
    info "Building CLP core .deb package (this may take a few minutes)..."
    "$PROJECT_DIR/scripts/build-clp.sh"
else
    info "CLP core .deb package found, skipping build"
fi

info "Building Docker image..."
$COMPOSE build coordinator-node --quiet 2>&1

info "Starting infrastructure (MariaDB, Kafka, MinIO)..."
DB_PORT="$DB_PORT" $COMPOSE up -d 2>&1

# Wait for MariaDB
wait_for_condition "MariaDB healthy" \
    "docker exec clp-mariadb mariadb-admin ping -uroot -ppassword 2>/dev/null | grep -q alive" \
    60 2 || { fail "MariaDB did not become healthy"; cleanup; exit 1; }

# Wait for Kafka
wait_for_condition "Kafka healthy" \
    "docker exec clp-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1" \
    30 2 || { fail "Kafka did not become healthy"; cleanup; exit 1; }

info "Infrastructure ready"

# =========================================================================
# Test 1: Fight-for-master
# =========================================================================
echo ""
info "=== Test 1: Fight-for-master ==="

info "Seeding _table registry with 'clp_spark' table (node_id = NULL)..."
db_exec "
INSERT IGNORE INTO _table (table_name, display_name, active) VALUES ('clp_spark', 'Spark Logs', true);
INSERT IGNORE INTO _table_kafka (table_name, kafka_bootstrap_servers, kafka_topic) VALUES ('clp_spark', 'kafka:29092', 'clp_spark');
INSERT IGNORE INTO _table_config (table_name) VALUES ('clp_spark');
INSERT IGNORE INTO _table_assignment (table_name, node_id) VALUES ('clp_spark', NULL);
"

# Verify seed
SEED_CHECK=$(db_exec "SELECT node_id FROM _table_assignment WHERE table_name = 'clp_spark'")
if [ "$SEED_CHECK" = "NULL" ] || [ -z "$SEED_CHECK" ]; then
    pass "clp_spark table seeded with node_id = NULL"
else
    fail "clp_spark table seed unexpected: node_id='$SEED_CHECK'"
fi

info "Restarting both nodes to trigger fight-for-master..."
$COMPOSE restart coordinator-node 2>&1

# Wait for the spark table to be claimed (node_id IS NOT NULL)
wait_for_condition "clp_spark table claimed" \
    "db_exec \"SELECT node_id FROM _table_assignment WHERE table_name = 'clp_spark' AND node_id IS NOT NULL\" | grep -q ." \
    45 3 || { fail "clp_spark table was not claimed"; $COMPOSE logs coordinator-node 2>&1 | tail -30; cleanup; exit 1; }

sleep 5  # Let both nodes finish startup fully

# Check assignment
ASSIGNED_NODE=$(db_exec "SELECT node_id FROM _table_assignment WHERE table_name = 'clp_spark'")
if [ -n "$ASSIGNED_NODE" ] && [ "$ASSIGNED_NODE" != "NULL" ]; then
    pass "clp_spark table claimed by node '$ASSIGNED_NODE'"
else
    fail "clp_spark table was not claimed (node_id='$ASSIGNED_NODE')"
fi

# Verify exactly one node claimed it
CLAIM_COUNT=$($COMPOSE logs coordinator-node 2>&1 | grep -c "Claimed table 'clp_spark'" || true)
SKIP_COUNT=$($COMPOSE logs coordinator-node 2>&1 | grep -c "Table 'clp_spark' already claimed" || true)

if [ "$CLAIM_COUNT" -eq 1 ]; then
    pass "Exactly 1 node claimed 'clp_spark' (no double-claim)"
else
    fail "Expected 1 claim for 'clp_spark', got $CLAIM_COUNT"
fi

if [ "$SKIP_COUNT" -ge 1 ]; then
    pass "Other node(s) saw 'clp_spark' already claimed and skipped"
else
    info "No skip log found (other node may not have raced for 'clp_spark')"
fi

# =========================================================================
# Test 2: Only one coordinator running per table
# =========================================================================
echo ""
info "=== Test 2: Single coordinator per table ==="

# Count how many times a coordinator unit was CREATED (not discovered)
# "Discovered" logs every DB query, "Created unit" logs only on actual creation
COORDINATOR_COUNT=$($COMPOSE logs coordinator-node 2>&1 | grep -c "Created unit: coordinator-clp_spark" || true)

if [ "$COORDINATOR_COUNT" -eq 1 ]; then
    pass "Exactly 1 coordinator created for 'clp_spark'"
else
    fail "Expected 1 coordinator for 'clp_spark', found $COORDINATOR_COUNT"
fi

# Identify which node is running the clp_spark coordinator
OWNER_LOG=$($COMPOSE logs coordinator-node 2>&1 | grep "Created unit: coordinator-clp_spark" || true)
info "Coordinator owner: $OWNER_LOG"

# =========================================================================
# Test 3: Kafka ingestion into clp_spark
# =========================================================================
echo ""
info "=== Test 3: Kafka metadata ingestion ==="

# Create the kafka topic explicitly (auto-create may lag)
docker exec clp-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic clp_spark \
    --partitions 1 \
    --replication-factor 1 2>/dev/null || true

info "Producing 3 test records to 'clp_spark' topic..."

kafka_produce "clp_spark" '{
  "ir_storage_backend": "s3",
  "ir_bucket": "clp-ir",
  "ir_path": "s3://clp-ir/app-001/executor-0/file-001.clp.zst",
  "state": "IR_ARCHIVE_BUFFERING",
  "min_timestamp": 1704067200,
  "max_timestamp": 1704067500,
  "record_count": 1000,
  "counts": {
    "agg_int/gte/level/debug": 900,
    "agg_int/gte/level/info": 700,
    "agg_int/gte/level/warn": 100,
    "agg_int/gte/level/error": 10,
    "agg_int/gte/level/fatal": 1
  },
  "raw_size_bytes": 5242880,
  "ir_size_bytes": 1048576,
  "retention_days": 30,
  "expires_at": 0
}'

kafka_produce "clp_spark" '{
  "ir_storage_backend": "s3",
  "ir_bucket": "clp-ir",
  "ir_path": "s3://clp-ir/app-001/executor-0/file-002.clp.zst",
  "state": "IR_ARCHIVE_BUFFERING",
  "min_timestamp": 1704067200,
  "max_timestamp": 1704067800,
  "record_count": 2500,
  "counts": {
    "agg_int/gte/level/debug": 2200,
    "agg_int/gte/level/info": 1800,
    "agg_int/gte/level/warn": 300,
    "agg_int/gte/level/error": 25,
    "agg_int/gte/level/fatal": 0
  },
  "raw_size_bytes": 10485760,
  "ir_size_bytes": 2097152,
  "retention_days": 30,
  "expires_at": 0
}'

kafka_produce "clp_spark" '{
  "ir_storage_backend": "minio",
  "ir_bucket": "clp-ir",
  "ir_path": "s3://clp-ir/app-002/executor-1/file-003.clp.zst",
  "state": "IR_ARCHIVE_CONSOLIDATION_PENDING",
  "min_timestamp": 1704153600,
  "max_timestamp": 1704154200,
  "record_count": 500,
  "counts": {
    "agg_int/gte/level/debug": 480,
    "agg_int/gte/level/info": 400,
    "agg_int/gte/level/warn": 50,
    "agg_int/gte/level/error": 5,
    "agg_int/gte/level/fatal": 0
  },
  "raw_size_bytes": 2621440,
  "ir_size_bytes": 524288,
  "retention_days": 7,
  "expires_at": 0
}'

info "Waiting for records to appear in clp_spark table..."

INGESTED=false
if wait_for_condition "records in clp_spark" \
    "[ \$(db_exec 'SELECT COUNT(*) FROM clp_spark' 2>/dev/null) -ge 3 ]" \
    30 2; then
    INGESTED=true
fi

if [ "$INGESTED" = true ]; then
    ROW_COUNT=$(db_exec "SELECT COUNT(*) FROM clp_spark")
    pass "Ingested $ROW_COUNT records into clp_spark"

    # Verify specific data
    info "Verifying ingested data..."

    TOTAL_RECORDS=$(db_exec "SELECT SUM(record_count) FROM clp_spark")
    if [ "$TOTAL_RECORDS" -eq 4000 ]; then
        pass "Total record_count = 4000 (1000 + 2500 + 500)"
    else
        fail "Expected total record_count = 4000, got $TOTAL_RECORDS"
    fi

    STATES=$(db_exec "SELECT DISTINCT state FROM clp_spark ORDER BY state" | tr '\n' ',')
    if echo "$STATES" | grep -q "IR_ARCHIVE_BUFFERING" && echo "$STATES" | grep -q "IR_ARCHIVE_CONSOLIDATION_PENDING"; then
        pass "Both states present: IR_ARCHIVE_BUFFERING, IR_ARCHIVE_CONSOLIDATION_PENDING"
    else
        fail "Expected IR_ARCHIVE_BUFFERING and IR_ARCHIVE_CONSOLIDATION_PENDING, got: $STATES"
    fi

    IR_PATHS=$(db_exec "SELECT clp_ir_path FROM clp_spark ORDER BY clp_ir_path")
    PATH_COUNT=$(echo "$IR_PATHS" | wc -l)
    if [ "$PATH_COUNT" -eq 3 ]; then
        pass "All 3 unique IR paths stored"
    else
        fail "Expected 3 IR paths, got $PATH_COUNT"
    fi

    info "Sample data:"
    db_exec "SELECT id, clp_ir_path, state, record_count, min_timestamp FROM clp_spark" | \
        while IFS=$'\t' read -r id path state count ts; do
            echo "  id=$id  state=$state  records=$count  path=$path"
        done
else
    fail "Records did not appear in clp_spark within timeout"
    info "Checking coordinator logs for errors..."
    $COMPOSE logs coordinator-node 2>&1 | grep -iE "ERROR|WARN|exception|Dropping" | tail -20
fi

# =========================================================================
# Test 4: Unique nodeIds from HOSTNAME
# =========================================================================
echo ""
info "=== Test 4: Unique nodeIds ==="

NODE_IDS=$($COMPOSE logs coordinator-node 2>&1 | grep "Resolved nodeId from env var HOSTNAME=" | sed 's/.*HOSTNAME=//' | sort -u || true)
NODE_COUNT=$(echo "$NODE_IDS" | grep -c . || true)

if [ "$NODE_COUNT" -eq 2 ]; then
    pass "2 unique nodeIds resolved from HOSTNAME"
    echo "$NODE_IDS" | while read -r nid; do
        echo "  nodeId=$nid"
    done
else
    fail "Expected 2 unique nodeIds, got $NODE_COUNT"
fi

# =========================================================================
# Test 5: Periodic reconciliation — table added after startup is picked up
# =========================================================================
echo ""
info "=== Test 5: Periodic reconciliation (table added after startup) ==="

info "Inserting 'clp_flink' table into registry with node_id = NULL..."
db_exec "
INSERT IGNORE INTO _table (table_name, display_name, active) VALUES ('clp_flink', 'Flink Logs', true);
INSERT IGNORE INTO _table_kafka (table_name, kafka_bootstrap_servers, kafka_topic) VALUES ('clp_flink', 'kafka:29092', 'clp_flink');
INSERT IGNORE INTO _table_config (table_name) VALUES ('clp_flink');
INSERT IGNORE INTO _table_assignment (table_name, node_id) VALUES ('clp_flink', NULL);
"

# Verify seed
FLINK_SEED=$(db_exec "SELECT node_id FROM _table_assignment WHERE table_name = 'clp_flink'")
if [ "$FLINK_SEED" = "NULL" ] || [ -z "$FLINK_SEED" ]; then
    pass "clp_flink seeded with node_id = NULL (no restart triggered)"
else
    fail "clp_flink seed unexpected: node_id='$FLINK_SEED'"
fi

# Wait for a node to claim it via periodic reconciliation (interval=5s, allow up to 30s)
if wait_for_condition "clp_flink claimed by reconciliation" \
    "db_exec \"SELECT node_id FROM _table_assignment WHERE table_name = 'clp_flink' AND node_id IS NOT NULL\" | grep -q ." \
    15 2; then
    FLINK_OWNER=$(db_exec "SELECT node_id FROM _table_assignment WHERE table_name = 'clp_flink'")
    pass "clp_flink claimed by node '$FLINK_OWNER' (no restart needed)"
else
    fail "clp_flink was NOT claimed within 30s — periodic reconciliation may not be working"
fi

# Verify coordinator was created and started for the new table.
# Dump logs to a temp file to avoid pipe buffering issues with large $COMPOSE output.
FLINK_LOG=$(mktemp)
if wait_for_condition "coordinator-clp_flink running" \
    "$COMPOSE logs coordinator-node >\"$FLINK_LOG\" 2>&1 && grep -q 'Created unit: coordinator-clp_flink' \"$FLINK_LOG\"" \
    10 2; then
    pass "CoordinatorUnit created for clp_flink after reconciliation"
else
    fail "No coordinator created for clp_flink"
    grep -i "clp_flink" "$FLINK_LOG" | grep -v DEBUG | tail -10
fi
rm -f "$FLINK_LOG"

# =========================================================================
# Summary
# =========================================================================
echo ""
echo "========================================="
if [ "$FAILURES" -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
else
    echo -e "${RED}$FAILURES test(s) failed${NC}"
fi
echo "========================================="

# =========================================================================
# Cleanup
# =========================================================================
echo ""
read -rp "Tear down the stack? [Y/n] " answer
if [ "${answer:-Y}" != "n" ] && [ "${answer:-Y}" != "N" ]; then
    cleanup
else
    info "Stack left running. Tear down manually with: $COMPOSE down -v"
fi

exit "$FAILURES"
