#!/usr/bin/env python3
"""
Unified ingestion benchmark for the Metalog coordinator.

Supports three ingestion modes:
  grpc        — send records via gRPC Ingest RPC
  kafka-proto — produce protobuf records to Kafka
  kafka-json  — produce JSON records to Kafka

Usage:
  ./integration-tests/benchmarks/ingestion/run.py [--mode MODE] [options]

Examples:
  ./integration-tests/benchmarks/ingestion/run.py --mode grpc -r 100000
  ./integration-tests/benchmarks/ingestion/run.py --mode kafka-proto -r 50000
  ./integration-tests/benchmarks/ingestion/run.py --mode kafka-json -r 50000
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
SCRIPT_DIR   = Path(__file__).parent.resolve()
PROJECT_DIR  = SCRIPT_DIR.parent.parent.parent.resolve()
COMPOSE_FILE = SCRIPT_DIR / "docker-compose.yml"
SERVER_BIN   = PROJECT_DIR / "bin" / "metalog-server"
BENCH_BIN    = PROJECT_DIR / "bin" / "benchmark-ingestion"

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
RED    = "\033[0;31m"
GREEN  = "\033[0;32m"
YELLOW = "\033[1;33m"
BLUE   = "\033[0;34m"
BOLD   = "\033[1m"
NC     = "\033[0m"

def log_info(msg):    print(f"{BLUE}[INFO]{NC} {msg}", flush=True)
def log_success(msg): print(f"{GREEN}[OK]{NC} {msg}", flush=True)
def log_warn(msg):    print(f"{YELLOW}[WARN]{NC} {msg}", flush=True)
def log_error(msg):   print(f"{RED}[ERROR]{NC} {msg}", file=sys.stderr, flush=True)

# ---------------------------------------------------------------------------
# Global state for cleanup
# ---------------------------------------------------------------------------
_coordinator_proc = None
_temp_config = None
_dc_cmd = None

def _cleanup():
    global _coordinator_proc, _temp_config, _dc_cmd
    print(flush=True)
    print(f"{YELLOW}[CLEANUP]{NC} Cleaning up...", flush=True)
    if _coordinator_proc is not None and _coordinator_proc.poll() is None:
        print(f"{YELLOW}[CLEANUP]{NC} Stopping coordinator (PID: {_coordinator_proc.pid})...", flush=True)
        _coordinator_proc.terminate()
        try:
            _coordinator_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            _coordinator_proc.kill()
    if _temp_config and Path(_temp_config).exists():
        Path(_temp_config).unlink()
    if _dc_cmd:
        print(f"{YELLOW}[CLEANUP]{NC} Tearing down containers...", flush=True)
        subprocess.run(_dc_cmd + ["down", "-v", "--remove-orphans"], capture_output=True)
    print(f"{YELLOW}[CLEANUP]{NC} Done", flush=True)

atexit.register(_cleanup)

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def load_env(path=".env"):
    p = PROJECT_DIR / path
    if p.exists():
        with open(p) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


def find_docker_compose():
    for cmd in (["docker", "compose"], ["docker-compose"]):
        try:
            if subprocess.run(cmd + ["version"], capture_output=True).returncode == 0:
                return cmd + ["-f", str(COMPOSE_FILE)]
        except (FileNotFoundError, OSError):
            pass
    log_error("Neither 'docker compose' nor 'docker-compose' found")
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


def wait_for_port(host, port, timeout=30):
    for i in range(1, timeout + 1):
        try:
            with socket.create_connection((host, port), timeout=1):
                return i
        except OSError:
            pass
        time.sleep(1)
    return None


def wait_for_port_free(port, timeout=15):
    for _ in range(timeout):
        try:
            with socket.create_connection(("localhost", port), timeout=0.5):
                pass
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
        return -1
    try:
        return int(r.stdout.strip())
    except (ValueError, AttributeError):
        return -1


def build_go():
    log_info("Building Go binaries...")
    bin_dir = PROJECT_DIR / "bin"
    bin_dir.mkdir(exist_ok=True)
    for target, output in [
        ("./cmd/metalog-server", str(SERVER_BIN)),
        ("./cmd/benchmark-ingestion", str(BENCH_BIN)),
    ]:
        r = subprocess.run(
            ["go", "build", "-o", output, target],
            cwd=PROJECT_DIR, env={**os.environ, "CGO_ENABLED": "1"},
        )
        if r.returncode != 0:
            log_error(f"Failed to build {target}")
            sys.exit(1)
    log_success("Build complete")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global _coordinator_proc, _temp_config, _dc_cmd

    parser = argparse.ArgumentParser(description="Unified ingestion benchmark")
    parser.add_argument("-m", "--mode",        default="grpc", choices=["grpc", "kafka-proto", "kafka-json"],
                        help="Ingestion mode (default: grpc)")
    parser.add_argument("-r", "--records",     type=int, default=100000,  help="Number of records (default: 100000)")
    parser.add_argument("-a", "--apps",        type=int, default=10000,   help="Distinct app IDs (default: 10000)")
    parser.add_argument("-c", "--concurrency", type=int, default=5000,    help="Max concurrent RPCs — grpc mode only (default: 5000)")
    parser.add_argument("-p", "--partitions",  type=int, default=2,       help="Kafka topic partitions — kafka modes only (default: 2)")
    parser.add_argument("-t", "--timeout",     type=int, default=300,     help="Timeout in seconds (default: 300)")
    parser.add_argument("-s", "--skip-infra",  action="store_true",       help="Skip infrastructure startup")
    parser.add_argument("--skip-clean",        action="store_true",       help="Skip clearing the database")
    args = parser.parse_args()

    load_env()

    db_port    = int(os.environ.get("DB_PORT",    3306))
    kafka_port = int(os.environ.get("KAFKA_PORT", 9092))
    grpc_port  = int(os.environ.get("GRPC_PORT",  9091))
    needs_kafka = args.mode.startswith("kafka")

    dc = find_docker_compose()
    _dc_cmd = dc

    print()
    print(f"{BOLD}==========================================")
    print(f"  Ingestion Benchmark — {args.mode}")
    print(f"=========================================={NC}")
    print()
    print("Configuration:")
    print(f"  Mode        : {args.mode}")
    print(f"  Records     : {args.records}")
    print(f"  Apps        : {args.apps}")
    if args.mode == "grpc":
        print(f"  Concurrency : {args.concurrency}")
        print(f"  gRPC port   : {grpc_port}")
    else:
        print(f"  Kafka port  : {kafka_port}")
        print(f"  Partitions  : {args.partitions}")
    print(f"  DB port     : {db_port}")
    print(f"  Timeout     : {args.timeout}s")
    print(f"  Skip infra  : {args.skip_infra}")
    print(f"  Skip clean  : {args.skip_clean}")
    print()

    # Step 1: Build
    build_go()

    # Step 2: Start infrastructure
    if not args.skip_infra:
        log_info("Stopping existing containers...")
        subprocess.run(dc + ["down", "-v", "--remove-orphans"], capture_output=True)
        subprocess.run(
            ["docker", "rm", "-f", "metalog-mariadb", "metalog-kafka", "metalog-zookeeper",
             "metalog-mariadb-scalability"],
            capture_output=True,
        )

        services = ["mariadb"]
        if needs_kafka:
            services += ["kafka", "zookeeper"]
        log_info(f"Starting infrastructure ({', '.join(services)})...")
        subprocess.run(dc + ["up", "-d"] + services, check=True)

        log_info("Waiting for MariaDB...")
        took = wait_for_mariadb(dc, db_port)
        if took is None:
            log_error("MariaDB failed to start within 180 seconds")
            sys.exit(1)
        log_success(f"MariaDB ready (took {took}s)")
        time.sleep(2)

        if needs_kafka:
            log_info("Waiting for Kafka...")
            took = wait_for_kafka(dc)
            if took is None:
                log_error("Kafka failed to start within 60 seconds")
                sys.exit(1)
            log_success("Kafka ready")

            # Create topic with configured partitions
            log_info(f"Creating topic 'clp_spark' with {args.partitions} partition(s)...")
            subprocess.run(
                dc + ["exec", "-T", "kafka", "kafka-topics",
                      "--bootstrap-server", "localhost:9092",
                      "--create", "--if-not-exists",
                      "--topic", "clp_spark",
                      "--partitions", str(args.partitions),
                      "--replication-factor", "1"],
                capture_output=True,
            )
            log_success(f"Topic ready ({args.partitions} partition(s))")
    else:
        log_warn("Skipping infrastructure startup")

    # Step 3: Clean table
    if not args.skip_clean:
        log_info("Dropping benchmark table...")
        subprocess.run(
            dc + ["exec", "-T", "mariadb", "mariadb", "-h", "127.0.0.1", "-uroot", "-ppassword", "metalog_metastore",
                  "-e", "DROP TABLE IF EXISTS clp_spark;"],
            capture_output=True, text=True,
        )
        log_success("Table dropped")
    else:
        log_warn("Skipping database cleanup")

    # Step 4: Write coordinator config
    kafka_section = ""
    grpc_section = ""
    if needs_kafka:
        kafka_section = f"""    kafka:
      topic: clp_spark
      bootstrapServers: localhost:{kafka_port}"""
    if args.mode == "grpc":
        grpc_section = f"""  grpc:
    enabled: true
    port: {grpc_port}"""

    with tempfile.NamedTemporaryFile(
        mode="w", prefix="metalog-bench-", suffix=".yaml", delete=False
    ) as f:
        _temp_config = f.name
        f.write(f"""\
node:
  name: benchmark-coordinator
  database:
    host: localhost
    port: {db_port}
    database: metalog_metastore
    user: root
    password: password
    poolSize: 20
    poolMinIdle: 5
{grpc_section}
  health:
    enabled: false
tables:
  - name: clp_spark
{kafka_section}
worker:
  numWorkers: 0
""")

    # Step 5: For Kafka modes, produce all records BEFORE starting coordinator.
    # This isolates consumer→DB ingestion throughput from producer speed.
    if needs_kafka:
        log_info(f"Producing {args.records} records to Kafka (before coordinator starts)...")
        produce_cmd = [str(BENCH_BIN), "--mode", args.mode,
                       "--records", str(args.records), "--apps", str(args.apps),
                       "--table", "clp_spark",
                       "--bootstrap-servers", f"localhost:{kafka_port}", "--topic", "clp_spark"]
        result = subprocess.run(produce_cmd)
        if result.returncode != 0:
            log_error("Producer failed")
            sys.exit(1)
        log_success(f"All {args.records} records produced to Kafka")

    # Step 6: Start coordinator
    if args.mode == "grpc":
        subprocess.run(["fuser", "-k", f"{grpc_port}/tcp"], capture_output=True)
        if not wait_for_port_free(grpc_port, timeout=15):
            log_error(f"gRPC port {grpc_port} still in use")
            sys.exit(1)

    log_info("Starting coordinator...")
    _coordinator_proc = subprocess.Popen([str(SERVER_BIN), "--config", _temp_config])
    log_info(f"Coordinator started (PID: {_coordinator_proc.pid})")

    if args.mode == "grpc":
        log_info(f"Waiting for gRPC endpoint on port {grpc_port}...")
        took = wait_for_port("localhost", grpc_port, timeout=30)
        if took is None:
            log_error("Coordinator did not open gRPC port within 30s")
            sys.exit(1)
        log_success(f"Coordinator gRPC ready (took {took}s)")

    # Step 7: Run benchmark / monitor ingestion
    if args.mode == "grpc":
        initial_count = get_row_count(dc, "clp_spark")
        log_info(f"Initial row count: {initial_count}")

        bench_cmd = [str(BENCH_BIN), "--mode", "grpc",
                     "--records", str(args.records), "--apps", str(args.apps),
                     "--table", "clp_spark",
                     "--grpc-host", "localhost", "--grpc-port", str(grpc_port),
                     "--concurrency", str(args.concurrency)]
        log_info(f"Sending {args.records} records via gRPC...")
        result = subprocess.run(bench_cmd)
        if result.returncode != 0:
            log_error("Benchmark binary failed")
            _stop_coordinator()
            sys.exit(1)

        # Wait for DB writes to flush
        log_info("Waiting for DB writes to flush...")
        passed = _wait_for_db(dc, initial_count, args.records, args.timeout)

    else:
        # Kafka: coordinator just started, records already in Kafka.
        # Time how long until all records appear in DB.
        log_info("Measuring Kafka consumer -> DB ingestion throughput...")
        ingest_start = time.time()
        passed = _wait_for_db(dc, 0, args.records, args.timeout)
        ingest_duration = time.time() - ingest_start

        if passed:
            throughput = args.records / ingest_duration
            print()
            print("==========================================")
            print(f"  KAFKA INGESTION RESULTS ({args.mode})")
            print("==========================================")
            print()
            print(f"  Records     : {args.records}")
            print(f"  Apps        : {args.apps} distinct")
            print(f"  Partitions  : {args.partitions}")
            print("  ----------------------------------------")
            print(f"  Ingestion (Kafka -> DB)")
            print(f"    Duration  : {int(ingest_duration * 1000)} ms  ({ingest_duration:.1f} s)")
            print(f"    Throughput: {int(throughput)} rec/s")
            print("==========================================")

    # Step 8: Stop coordinator
    _stop_coordinator()
    sys.exit(0 if passed else 1)


def _wait_for_db(dc, initial_count, target_records, timeout):
    """Poll DB until row count reaches initial_count + target_records. Returns True on success."""
    expected = initial_count + target_records
    elapsed = 0
    while elapsed < timeout:
        current = get_row_count(dc, "clp_spark")
        inserted = current - initial_count
        if elapsed % 5 == 0:
            pct = f"{inserted * 100 / target_records:.1f}" if target_records > 0 else "0"
            log_info(f"Progress: {inserted}/{target_records} records in DB ({pct}%)")
        if current >= expected:
            total = current - initial_count
            status = f"{GREEN}PASS{NC}"
            print()
            print(f"  DB Verification: {status}")
            print(f"  Records in DB : {total} / {target_records}")
            print()
            return True
        time.sleep(1)
        elapsed += 1

    current = get_row_count(dc, "clp_spark")
    total = current - initial_count
    status = f"{RED}FAIL ({total}/{target_records}){NC}"
    print()
    print(f"  DB Verification: {status}")
    print(f"  Records in DB : {total} / {target_records}")
    print()
    return False


def _stop_coordinator():
    global _coordinator_proc
    if _coordinator_proc and _coordinator_proc.poll() is None:
        log_info("Stopping coordinator...")
        _coordinator_proc.terminate()
        try:
            _coordinator_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            _coordinator_proc.kill()
    _coordinator_proc = None


if __name__ == "__main__":
    main()
