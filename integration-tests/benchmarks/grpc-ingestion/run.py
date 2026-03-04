#!/usr/bin/env python3
"""
End-to-end gRPC ingestion throughput benchmark for the Metalog coordinator.

Steps:
  1. Start infrastructure (MariaDB only)
  2. Create the benchmark table schema
  3. Start the coordinator with gRPC ingestion enabled
  4. Send configurable number of records via Ingest RPC (one record per RPC)
  5. Monitor database until all records are inserted
  6. Report throughput metrics

Usage:
  ./benchmarks/grpc-ingestion/run.py [options]

Options:
  -r, --records       Number of records to send (default: 100000)
  -a, --apps          Number of distinct app IDs (default: 10)
  -c, --concurrency   Max concurrent in-flight RPCs (default: 5000)
  -t, --timeout       Timeout in seconds (default: 300)
  -s, --skip-infra    Skip infrastructure startup (reuse existing containers)
  --skip-clean        Skip clearing the database
  -m, --mode          Wire format: structured (DimEntry/AggEntry) or compat (SelfDescribingEntry)
                      (default: structured)
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

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_DIR = SCRIPT_DIR.parent.parent.parent.resolve()
COMPOSE_FILE = SCRIPT_DIR / "docker-compose.yml"
COORDINATOR_JAR = PROJECT_DIR / "grpc-server" / "target" / "metalog-grpc-server-1.0-SNAPSHOT.jar"
BENCHMARK_JAR = PROJECT_DIR / "benchmarks" / "target" / "metalog-benchmarks-1.0-SNAPSHOT.jar"
CLASSPATH = f"{COORDINATOR_JAR}:{BENCHMARK_JAR}"
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
    subprocess.run(["pkill", "-f", COORDINATOR_JAR.name], capture_output=True)
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


def wait_for_mariadb(dc, port, timeout=180):
    for i in range(1, timeout + 1):
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                pass
        except OSError:
            if i % 10 == 0:
                log_info(f"Still waiting for MariaDB... ({i}s)")
            time.sleep(1)
            continue
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


def wait_for_port(host, port, timeout=30, label=None):
    label = label or f"{host}:{port}"
    for i in range(1, timeout + 1):
        try:
            with socket.create_connection((host, port), timeout=1):
                return i
        except OSError:
            pass
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


def get_row_count(dc, table):
    r = subprocess.run(
        dc + ["exec", "-T", "mariadb", "mariadb", "-h", "127.0.0.1", "-uroot", "-ppassword", "metalog_metastore",
              "-sN", "-e", f"SELECT COUNT(*) FROM {table};"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        log_warn(f"DB query failed (rc={r.returncode}): {r.stderr.strip()}")
        return -1
    try:
        return int(r.stdout.strip())
    except (ValueError, AttributeError):
        log_warn(f"Could not parse row count from: {r.stdout!r}")
        return -1


def build_jar():
    log_info("Building...")
    cmd = ["mvn", "clean", "package", "-DskipTests", "-q"]
    if SETTINGS_FILE.exists():
        cmd += ["--settings", str(SETTINGS_FILE)]
    subprocess.run(cmd, cwd=PROJECT_DIR, check=True)
    if not COORDINATOR_JAR.exists():
        log_error(f"JAR not found: {COORDINATOR_JAR}")
        sys.exit(1)
    if not BENCHMARK_JAR.exists():
        log_error(f"JAR not found: {BENCHMARK_JAR}")
        sys.exit(1)
    log_success("Build complete")


def now_ms():
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global _coordinator_proc, _temp_config

    parser = argparse.ArgumentParser(description="gRPC ingestion benchmark")
    parser.add_argument("-r", "--records",     type=int, default=100000,      help="Number of records (default: 100000)")
    parser.add_argument("-a", "--apps",        type=int, default=10,          help="Distinct app IDs (default: 10)")
    parser.add_argument("-c", "--concurrency", type=int, default=5000,        help="Max concurrent RPCs (default: 5000)")
    parser.add_argument("-t", "--timeout",     type=int, default=300,         help="Timeout in seconds (default: 300)")
    parser.add_argument("-m", "--mode",        default="structured",          help="Wire format: structured or compat (default: structured)")
    parser.add_argument("-s", "--skip-infra", action="store_true",            help="Skip infrastructure startup")
    parser.add_argument("--skip-clean",       action="store_true",            help="Skip clearing the database")
    args = parser.parse_args()

    load_env()

    db_port   = int(os.environ.get("DB_PORT",   3306))
    grpc_port = int(os.environ.get("GRPC_PORT", 9091))

    dc = find_docker_compose()

    print()
    print("==========================================")
    print("  gRPC Ingestion Benchmark")
    print("==========================================")
    print()
    print("Configuration:")
    print(f"  Records     : {args.records}")
    print(f"  Apps        : {args.apps}")
    print(f"  Concurrency : {args.concurrency} concurrent RPCs")
    print(f"  Mode        : {args.mode}")
    print(f"  Timeout     : {args.timeout}s")
    print(f"  gRPC port   : {grpc_port}")
    print(f"  DB port     : {db_port}")
    print(f"  Skip infra  : {args.skip_infra}")
    print(f"  Skip clean  : {args.skip_clean}")
    print()

    # Step 1: Build
    build_jar()

    # Step 2: Manage containers
    if not args.skip_infra:
        log_info("Stopping existing containers...")
        subprocess.run(dc + ["down", "-v", "--remove-orphans"], capture_output=True)
        subprocess.run(
            ["docker", "rm", "-f", "metalog-mariadb", "metalog-mariadb-task-queue", "metalog-mariadb-scalability"],
            capture_output=True,
        )

        log_info("Starting infrastructure (MariaDB)...")
        subprocess.run(dc + ["up", "-d", "mariadb"], check=True)

        log_info("Waiting for MariaDB (up to 180 seconds)...")
        took = wait_for_mariadb(dc, db_port, timeout=180)
        if took is None:
            log_error("MariaDB failed to start within 180 seconds")
            sys.exit(1)
        log_success(f"MariaDB ready on port {db_port} (took {took}s)")
        time.sleep(2)
        log_success("Infrastructure ready")
    else:
        log_warn("Skipping infrastructure startup")

    # Step 3: Drop existing table (coordinator will re-create via TableProvisioner)
    if not args.skip_clean:
        log_info("Dropping benchmark table (coordinator will provision on startup)...")
        r = subprocess.run(
            dc + ["exec", "-T", "mariadb", "mariadb", "-h", "127.0.0.1", "-uroot", "-ppassword", "metalog_metastore",
                  "-e", "DROP TABLE IF EXISTS clp_spark;"],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            log_warn(f"DROP TABLE may have failed: {r.stderr.strip()}")
        else:
            log_success("Table dropped")
    else:
        log_warn("Skipping database cleanup")

    # Step 4: Write temp coordinator config with gRPC enabled
    with tempfile.NamedTemporaryFile(
        mode="w", prefix="metalog-grpc-benchmark-", suffix=".yaml", delete=False
    ) as f:
        _temp_config = f.name
        f.write(f"""\
node:
  database:
    host: localhost
    port: {db_port}
    database: metalog_metastore
    user: root
    password: password
    poolSize: 20
    poolMinIdle: 5
    connectionTimeout: 30000
    idleTimeout: 600000
  grpc:
    enabled: true
    port: {grpc_port}
  health:
    enabled: false
tables:
  - name: clp_spark
worker:
  numWorkers: 0
""")
    log_info(f"Wrote coordinator config to {_temp_config}")

    # Step 5: Start coordinator — free the gRPC port first
    # Kill any local coordinator process (java -jar or java -cp with this JAR)
    subprocess.run(["pkill", "-f", COORDINATOR_JAR.name], capture_output=True)
    # Stop any Docker container that publishes the gRPC port (e.g. presto-stack coordinator)
    r = subprocess.run(
        ["docker", "ps", "-q", "--filter", f"publish={grpc_port}"],
        capture_output=True, text=True,
    )
    if r.stdout.strip():
        log_info(f"Stopping Docker container(s) holding port {grpc_port}: {r.stdout.strip()}")
        subprocess.run(["docker", "stop"] + r.stdout.strip().split(), capture_output=True)
    subprocess.run(["fuser", "-k", f"{grpc_port}/tcp"], capture_output=True)
    if not wait_for_port_free(grpc_port, timeout=15):
        log_error(f"gRPC port {grpc_port} still in use after 15s — kill the old coordinator manually")
        sys.exit(1)

    log_info(f"Starting coordinator (gRPC on port {grpc_port})...")
    _coordinator_proc = subprocess.Popen(["java", "-jar", str(COORDINATOR_JAR), _temp_config])
    log_info(f"Coordinator started (PID: {_coordinator_proc.pid})")

    log_info("Waiting for gRPC endpoint (up to 30 seconds)...")
    for i in range(1, 31):
        if _coordinator_proc.poll() is not None:
            log_error("Coordinator exited unexpectedly")
            sys.exit(1)
        try:
            with socket.create_connection(("localhost", grpc_port), timeout=1):
                log_success(f"Coordinator gRPC ready (took {i}s)")
                break
        except OSError:
            pass
        if i == 30:
            log_error(f"Coordinator did not open gRPC port within 30s")
            sys.exit(1)
        time.sleep(1)

    initial_count = get_row_count(dc, "clp_spark")
    log_info(f"Initial row count: {initial_count}")

    # Step 6: Send records via gRPC
    log_info(f"Sending {args.records} records via gRPC (concurrency={args.concurrency})...")
    produce_start = now_ms()

    env = os.environ.copy()
    env["GRPC_HOST"] = "localhost"
    env["GRPC_PORT"] = str(grpc_port)
    env["TABLE_NAME"] = "clp_spark"
    subprocess.run(
        ["java", "-cp", CLASSPATH,
         "com.yscope.clp.service.tools.benchmark.GrpcBenchmarkProducer",
         "--records", str(args.records),
         "--apps", str(args.apps),
         "--concurrency", str(args.concurrency),
         "--mode", args.mode],
        env=env, check=True,
    )

    produce_end = now_ms()
    produce_duration = produce_end - produce_start
    produce_throughput = int(args.records * 1000 / produce_duration) if produce_duration > 0 else 0
    log_success(f"gRPC send finished in {produce_duration}ms ({produce_throughput} rec/s)")

    # Step 7: Wait for DB writes to flush
    log_info("Waiting for DB writes to flush...")
    monitor_start = now_ms()
    expected_count = initial_count + args.records
    elapsed = 0

    while elapsed < args.timeout:
        current_count = get_row_count(dc, "clp_spark")
        inserted = current_count - initial_count
        percent = f"{inserted * 100 / args.records:.1f}" if args.records > 0 else "0"
        if elapsed % 5 == 0:
            log_info(f"Progress: {inserted}/{args.records} records in DB ({percent}%)")
        if current_count >= expected_count:
            break
        time.sleep(1)
        elapsed += 1

    monitor_end = now_ms()
    monitor_duration = monitor_end - monitor_start

    final_count = get_row_count(dc, "clp_spark")
    total_inserted = final_count - initial_count

    # Step 8: Stop coordinator
    log_info("Stopping coordinator...")
    if _coordinator_proc and _coordinator_proc.poll() is None:
        _coordinator_proc.terminate()
        try:
            _coordinator_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            _coordinator_proc.kill()
    _coordinator_proc = None
    time.sleep(1)

    # Step 9: Print results
    produce_sec = produce_duration / 1000
    monitor_sec = monitor_duration / 1000
    passed = total_inserted >= args.records
    status = f"{GREEN}PASS{NC}" if passed else f"{RED}FAIL (timeout — {total_inserted} / {args.records} inserted){NC}"

    print()
    print("==========================================")
    print("  GRPC INGESTION BENCHMARK RESULTS")
    print("==========================================")
    print()
    print(f"  Status      : {status}")
    print(f"  Records     : {total_inserted} / {args.records}")
    print(f"  Apps        : {args.apps} distinct")
    print(f"  Concurrency : {args.concurrency} concurrent RPCs")
    print(f"  Mode        : {args.mode}")
    print( "  ----------------------------------------")
    print( "  gRPC send (producer + coordinator pipeline)")
    print(f"    Duration  : {produce_duration} ms  ({produce_sec:.1f} s)")
    print(f"    Throughput: {produce_throughput} rec/s")
    print( "  ----------------------------------------")
    print( "  DB flush (time for writes to commit)")
    print(f"    Duration  : {monitor_duration} ms  ({monitor_sec:.1f} s)")
    print()
    print("==========================================")
    print()

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
