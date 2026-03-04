#!/usr/bin/env python3
"""
End-to-end Kafka ingestion throughput benchmark for the CLP Service coordinator.

Steps:
  1. Start infrastructure (MariaDB, Kafka, ZooKeeper)
  2. Recreate the benchmark table schema
  3. Start the coordinator
  4. Produce configurable number of records to Kafka
  5. Monitor database until all records are inserted
  6. Report throughput metrics

Usage:
  ./benchmarks/kafka-ingestion/run.py [options]

Options:
  -r, --records <n>    Number of records to produce (default: 10000)
  -a, --apps <n>       Number of distinct app IDs (default: 10)
  -t, --timeout <sec>  Timeout in seconds (default: 120)
  -m, --mode <mode>    Wire format: json|proto-structured|proto-compat (default: json)
  -s, --skip-infra     Skip infrastructure startup (reuse existing containers)
  -c, --skip-clean     Skip clearing the database
"""
import argparse
import atexit
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

MODES = ["json", "proto-structured", "proto-compat"]

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_DIR = SCRIPT_DIR.parent.parent.parent.resolve()
COMPOSE_FILE = SCRIPT_DIR / "docker-compose.yml"
JAR_FILE = PROJECT_DIR / "target" / "clp-service-1.0-SNAPSHOT.jar"
SETTINGS_FILE = PROJECT_DIR / "settings-maven-central.xml"

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
RED    = "\033[0;31m"
GREEN  = "\033[0;32m"
YELLOW = "\033[1;33m"
BLUE   = "\033[0;34m"
NC     = "\033[0m"

def log_info(msg):    print(f"{BLUE}[INFO]{NC} {msg}", flush=True)
def log_success(msg): print(f"{GREEN}[SUCCESS]{NC} {msg}", flush=True)
def log_warn(msg):    print(f"{YELLOW}[WARN]{NC} {msg}", flush=True)
def log_error(msg):   print(f"{RED}[ERROR]{NC} {msg}", file=sys.stderr, flush=True)

# ---------------------------------------------------------------------------
# Global state for cleanup
# ---------------------------------------------------------------------------
_coordinator_proc = None
_temp_config = None

def _cleanup():
    global _coordinator_proc, _temp_config
    print(flush=True)
    print(f"{YELLOW}[CLEANUP]{NC} Cleaning up...", flush=True)
    if _coordinator_proc is not None and _coordinator_proc.poll() is None:
        print(f"{YELLOW}[CLEANUP]{NC} Stopping coordinator (PID: {_coordinator_proc.pid})...", flush=True)
        _coordinator_proc.terminate()
        try:
            _coordinator_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            _coordinator_proc.kill()
    subprocess.run(["pkill", "-f", JAR_FILE.name], capture_output=True)
    if _temp_config and Path(_temp_config).exists():
        Path(_temp_config).unlink()
    print(f"{YELLOW}[CLEANUP]{NC} Done", flush=True)

atexit.register(_cleanup)

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def load_env(path=".env"):
    p = PROJECT_DIR / path
    if p.exists():
        log_info(f"Loading port configuration from {path}")
        with open(p) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


def find_docker_compose():
    try:
        if subprocess.run(["docker-compose", "--version"], capture_output=True).returncode == 0:
            return ["docker-compose", "-f", str(COMPOSE_FILE)]
    except (FileNotFoundError, OSError):
        pass
    try:
        if subprocess.run(["docker", "compose", "version"], capture_output=True).returncode == 0:
            return ["docker", "compose", "-f", str(COMPOSE_FILE)]
    except (FileNotFoundError, OSError):
        pass
    log_error("Neither 'docker-compose' nor 'docker compose' found")
    sys.exit(1)


def wait_for_port(host, port, timeout=180, label=None):
    label = label or f"{host}:{port}"
    for i in range(1, timeout + 1):
        try:
            with socket.create_connection((host, port), timeout=1):
                return i
        except OSError:
            pass
        if i % 10 == 0:
            log_info(f"Still waiting for {label}... ({i}s)")
        time.sleep(1)
    return None


def wait_for_port_free(port, timeout=15):
    """Wait until the given local TCP port is no longer in use."""
    for _ in range(timeout):
        try:
            with socket.create_connection(("localhost", port), timeout=0.5):
                pass  # still in use
        except OSError:
            return True
        time.sleep(1)
    return False


def wait_for_mariadb(dc, port, timeout=180):
    for i in range(1, timeout + 1):
        # First check TCP
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                pass
        except OSError:
            if i % 10 == 0:
                log_info(f"Still waiting for MariaDB... ({i}s)")
            time.sleep(1)
            continue
        # Then check admin ping
        r = subprocess.run(
            dc + ["exec", "-T", "mariadb", "mariadb-admin", "ping", "-h", "127.0.0.1", "-uroot", "-ppassword"],
            capture_output=True,
        )
        if r.returncode == 0:
            return i
        if i % 10 == 0:
            log_info(f"Still waiting for MariaDB... ({i}s)")
        time.sleep(1)
    return None


def wait_for_kafka(dc, timeout=60):
    for i in range(1, timeout + 1):
        r = subprocess.run(
            dc + ["exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"],
            capture_output=True,
        )
        if r.returncode == 0:
            return i
        time.sleep(1)
    return None


def get_row_count(dc, table):
    r = subprocess.run(
        dc + ["exec", "-T", "mariadb", "mariadb", "-h", "127.0.0.1", "-uroot", "-ppassword", "clp_metastore",
              "-sN", "-e", f"SELECT COUNT(*) FROM {table};"],
        capture_output=True, text=True,
    )
    try:
        return int(r.stdout.strip())
    except (ValueError, AttributeError):
        return 0


def build_jar():
    log_info("Building coordinator...")
    cmd = ["mvn", "clean", "package", "-DskipTests", "-q"]
    if SETTINGS_FILE.exists():
        log_info("Using custom Maven settings (Maven Central)")
        cmd += ["--settings", str(SETTINGS_FILE)]
    else:
        log_warn("Using default Maven configuration")
    subprocess.run(cmd, cwd=PROJECT_DIR, check=True)
    if not JAR_FILE.exists():
        log_error(f"JAR not found: {JAR_FILE}")
        sys.exit(1)
    log_success("Build complete")


def now_ms():
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global _coordinator_proc, _temp_config

    parser = argparse.ArgumentParser(description="Kafka ingestion benchmark")
    parser.add_argument("-r", "--records",    type=int, default=10000,  help="Number of records (default: 10000)")
    parser.add_argument("-a", "--apps",       type=int, default=10,     help="Distinct app IDs (default: 10)")
    parser.add_argument("-t", "--timeout",    type=int, default=120,    help="Timeout in seconds (default: 120)")
    parser.add_argument("-m", "--mode",       default="json", choices=MODES, help="Wire format (default: json)")
    parser.add_argument("-s", "--skip-infra", action="store_true",      help="Skip infrastructure startup")
    parser.add_argument("-c", "--skip-clean", action="store_true",      help="Skip clearing the database")
    args = parser.parse_args()

    load_env()

    db_port    = int(os.environ.get("DB_PORT",    3306))
    kafka_port = int(os.environ.get("KAFKA_PORT", 9092))

    dc = find_docker_compose()

    print()
    print("==========================================")
    print("  Async Consolidation Benchmark")
    print("==========================================")
    print()
    print("Configuration:")
    print(f"  Records:     {args.records}")
    print(f"  Apps:        {args.apps}")
    print(f"  Timeout:     {args.timeout}s")
    print(f"  Mode:        {args.mode}")
    print(f"  Database:    MariaDB")
    print(f"  Skip infra:  {args.skip_infra}")
    print(f"  Skip clean:  {args.skip_clean}")
    print(f"  DB port:     {db_port}")
    print(f"  Kafka port:  {kafka_port}")
    print()

    # Step 1: Build
    build_jar()

    # Step 2: Manage containers
    if not args.skip_infra:
        log_info("Stopping and removing existing containers...")
        subprocess.run(dc + ["down", "-v", "--remove-orphans"], capture_output=True)
        subprocess.run(
            ["docker", "rm", "-f", "clp-mariadb", "clp-kafka", "clp-zookeeper",
             "clp-minio", "clp-minio-init", "clp-coordinator",
             "clp-mariadb-task-queue", "clp-mariadb-scalability"],
            capture_output=True,
        )
        log_success("Containers cleaned")
    else:
        subprocess.run(["docker", "rm", "-f", "clp-coordinator"], capture_output=True)

    # Step 3: Start infrastructure
    if not args.skip_infra:
        log_info("Starting infrastructure (MariaDB, Kafka)...")
        subprocess.run(dc + ["up", "-d", "mariadb", "kafka", "zookeeper"], check=True)

        log_info("Waiting for MariaDB (up to 180 seconds)...")
        took = wait_for_mariadb(dc, db_port, timeout=180)
        if took is None:
            log_error("MariaDB failed to start within 180 seconds")
            sys.exit(1)
        log_success(f"MariaDB ready on port {db_port} (took {took}s)")
        time.sleep(2)

        log_info("Waiting for Kafka...")
        took = wait_for_kafka(dc, timeout=60)
        if took is None:
            log_error("Kafka failed to start within 60 seconds")
            sys.exit(1)
        log_success("Kafka ready")
        log_success("Infrastructure ready")
    else:
        log_warn("Skipping infrastructure startup")

    # Step 4: Drop existing table (coordinator will re-create via TableProvisioner)
    if not args.skip_clean:
        log_info("Dropping benchmark table (coordinator will provision on startup)...")
        subprocess.run(
            dc + ["exec", "-T", "mariadb", "mariadb", "-h", "127.0.0.1", "-uroot", "-ppassword", "clp_metastore",
                  "-e", "DROP TABLE IF EXISTS clp_spark;"],
            capture_output=True,
        )
        log_success("Table dropped")
    else:
        log_warn("Skipping database cleanup")

    # Step 5: Write temp coordinator config and start coordinator
    with tempfile.NamedTemporaryFile(
        mode="w", prefix="clp-kafka-benchmark-", suffix=".yaml", delete=False
    ) as f:
        _temp_config = f.name
        f.write(f"""\
node:
  database:
    host: localhost
    port: {db_port}
    database: clp_metastore
    user: root
    password: password
    poolSize: 20
    poolMinIdle: 5
    connectionTimeout: 30000
    idleTimeout: 600000
  ingestion:
    kafka:
      recordsPerBatch: 500
      maxQueuedRecords: 10000
      flushTimeoutMs: 1000
  health:
    enabled: false
tables:
  - name: clp_spark
    kafka:
      topic: clp_spark
      bootstrapServers: localhost:{kafka_port}
worker:
  numWorkers: 0
""")

    log_info("Starting coordinator...")
    # pkill by JAR filename covers both "java -jar <jar>" and "java -cp <jar>" invocations.
    subprocess.run(["pkill", "-f", JAR_FILE.name], capture_output=True)
    time.sleep(1)

    _coordinator_proc = subprocess.Popen(["java", "-jar", str(JAR_FILE), _temp_config])
    log_info(f"Coordinator started (PID: {_coordinator_proc.pid})")

    log_info("Waiting for coordinator to initialize...")
    for i in range(1, 16):
        if _coordinator_proc.poll() is not None:
            log_error("Coordinator failed to start (process exited)")
            sys.exit(1)
        time.sleep(1)
    log_success("Coordinator running")

    initial_count = get_row_count(dc, "clp_spark")
    log_info(f"Initial row count: {initial_count}")

    # Step 6: Produce records
    log_info(f"Producing {args.records} records to Kafka...")
    producer_start = now_ms()

    env = os.environ.copy()
    env["KAFKA_TOPIC"] = "clp_spark"
    env["KAFKA_BOOTSTRAP_SERVERS"] = f"localhost:{kafka_port}"
    subprocess.run(
        ["java", "-cp", str(JAR_FILE),
         "com.yscope.clp.service.tools.benchmark.BenchmarkProducer",
         "--records", str(args.records),
         "--apps", str(args.apps),
         "--batch-size", "1000",
         "--mode", args.mode],
        env=env, check=True,
    )

    producer_end = now_ms()
    producer_duration = producer_end - producer_start
    producer_throughput = int(args.records * 1000 / producer_duration) if producer_duration > 0 else 0
    log_success(f"Producer finished in {producer_duration}ms ({producer_throughput} rec/sec)")

    # Step 7: Monitor until all records inserted
    log_info("Monitoring coordinator throughput...")
    monitor_start = now_ms()
    expected_count = initial_count + args.records
    elapsed = 0

    while elapsed < args.timeout:
        current_count = get_row_count(dc, "clp_spark")
        inserted = current_count - initial_count
        percent = f"{inserted * 100 / args.records:.1f}" if args.records > 0 else "0"
        if elapsed % 5 == 0:
            log_info(f"Progress: {inserted}/{args.records} records inserted ({percent}%)")
        if current_count >= expected_count:
            break
        time.sleep(1)
        elapsed += 1

    monitor_end = now_ms()
    monitor_duration = monitor_end - monitor_start

    final_count = get_row_count(dc, "clp_spark")
    total_inserted = final_count - initial_count
    coordinator_throughput = (
        int(total_inserted * 1000 / monitor_duration) if monitor_duration > 0 else 0
    )

    # Step 8: Stop coordinator
    log_info("Stopping coordinator...")
    if _coordinator_proc and _coordinator_proc.poll() is None:
        _coordinator_proc.terminate()
        try:
            _coordinator_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            _coordinator_proc.kill()
    _coordinator_proc = None
    time.sleep(2)

    # Step 9: Print results
    monitor_sec = monitor_duration / 1000
    passed = total_inserted >= args.records
    status = f"{GREEN}PASS{NC}" if passed else f"{RED}FAIL (timeout — {total_inserted} / {args.records} inserted){NC}"

    print()
    print("==========================================")
    print("  INGESTION BENCHMARK RESULTS")
    print("==========================================")
    print()
    print(f"  Status      : {status}")
    print(f"  Records     : {total_inserted} / {args.records}")
    print(f"  Apps        : {args.apps} distinct")
    print(f"  Mode        : {args.mode}")
    print( "  ----------------------------------------")
    print( "  Producer")
    print(f"    Duration  : {producer_duration} ms")
    print(f"    Throughput: {producer_throughput} rec/s")
    print( "  ----------------------------------------")
    print( "  Coordinator")
    print(f"    Duration  : {monitor_duration} ms  ({monitor_sec:.1f} s)")
    print(f"    Throughput: {coordinator_throughput} rec/s")
    print()
    print("==========================================")
    print()

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
