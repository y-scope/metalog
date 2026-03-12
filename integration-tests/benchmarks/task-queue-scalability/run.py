#!/usr/bin/env python3
"""
Task Queue Scalability Benchmark.

Sweeps node counts × batch sizes to find the throughput ceiling of the task queue.
Each node has 1 prefetcher (DB claimer) and N worker goroutines — matching production.

Usage:
  ./integration-tests/benchmarks/task-queue-scalability/run.py [options]

Options:
  -n, --nodes <csv>              Comma-separated node counts (default: 1,2,4,8,16,32)
  -W, --workers-per-node <csv>   Workers-per-node counts CSV (default: 4,8,16)
  -m, --batch-multiplier <n>     Batch = workers-per-node × multiplier (default: 1)
  -t, --tasks <n>                Tasks per worker (default: 50)
  -T, --tables <n>               Number of tables (default: 10)
  -s, --skip-infra               Skip infrastructure startup (reuse existing containers)
"""
import argparse
import atexit
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR  = Path(__file__).parent.resolve()
PROJECT_DIR = SCRIPT_DIR.parent.parent.parent.resolve()
COMPOSE_FILE = SCRIPT_DIR / "docker-compose.yml"
BENCHMARK_BIN = PROJECT_DIR / "bin" / "benchmark-taskqueue"

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
def log_success(msg): print(f"{GREEN}[SUCCESS]{NC} {msg}", flush=True)
def log_warn(msg):    print(f"{YELLOW}[WARN]{NC} {msg}", flush=True)
def log_error(msg):   print(f"{RED}[ERROR]{NC} {msg}", file=sys.stderr, flush=True)

# ---------------------------------------------------------------------------
# Global state for cleanup
# ---------------------------------------------------------------------------
_dc_cmd = None

def _cleanup():
    global _dc_cmd
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
        log_info(f"Loading port configuration from {path}")
        with open(p) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


def find_docker_compose():
    try:
        if subprocess.run(["docker", "compose", "version"], capture_output=True).returncode == 0:
            return ["docker", "compose", "-f", str(COMPOSE_FILE)]
    except (FileNotFoundError, OSError):
        pass
    try:
        if subprocess.run(["docker-compose", "--version"], capture_output=True).returncode == 0:
            return ["docker-compose", "-f", str(COMPOSE_FILE)]
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


def build_go():
    log_info("Building Go benchmark binary...")
    bin_dir = PROJECT_DIR / "bin"
    bin_dir.mkdir(exist_ok=True)
    r = subprocess.run(
        ["go", "build", "-o", str(BENCHMARK_BIN), "./cmd/benchmark-taskqueue"],
        cwd=PROJECT_DIR, env={**os.environ, "CGO_ENABLED": "1"},
    )
    if r.returncode != 0:
        log_error("Failed to build benchmark-taskqueue")
        sys.exit(1)
    log_success("Build complete")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global _dc_cmd

    parser = argparse.ArgumentParser(description="Task queue scalability benchmark")
    parser.add_argument("-n", "--nodes",             default="1,2,4,8,16,32", help="Node counts CSV")
    parser.add_argument("-W", "--workers-per-node",  default="1,2,4,8,16,32", help="Workers-per-node counts CSV")
    parser.add_argument("-m", "--batch-multiplier",  type=int, default=1,    help="Batch = workers-per-node × multiplier")
    parser.add_argument("-t", "--tasks",             type=int, default=50,   help="Tasks per worker (default: 50)")
    parser.add_argument("-T", "--tables",            type=int, default=10,   help="Number of tables (default: 10)")
    parser.add_argument("-s", "--skip-infra",        action="store_true",    help="Skip infrastructure startup")
    args = parser.parse_args()

    load_env()
    db_port = int(os.environ.get("DB_PORT", 3306))
    dc = find_docker_compose()
    _dc_cmd = dc

    print()
    print(f"{BOLD}╔════════════════════════════════════════════════════════════════════╗")
    print( "║           TASK QUEUE SCALABILITY BENCHMARK (Go)                    ║")
    print(f"╚════════════════════════════════════════════════════════════════════╝{NC}")
    print()
    print("Configuration:")
    print(f"  Nodes:             {args.nodes}")
    print(f"  Workers per node:  {args.workers_per_node}")
    print(f"  Batch multiplier:  {args.batch_multiplier}")
    print(f"  Tasks per worker:  {args.tasks}")
    print(f"  Tables:            {args.tables}")
    print(f"  DB port:           {db_port}")
    print(f"  Skip infra:        {args.skip_infra}")
    print()

    # Step 1: Build
    build_go()

    # Step 2: Start MariaDB
    if not args.skip_infra:
        log_info("Stopping existing containers...")
        subprocess.run(dc + ["down", "-v", "--remove-orphans"], capture_output=True)
        subprocess.run(
            ["docker", "rm", "-f", "metalog-mariadb", "metalog-kafka", "metalog-zookeeper",
             "metalog-mariadb-scalability"],
            capture_output=True,
        )

        log_info("Starting MariaDB (max_connections=2000)...")
        subprocess.run(dc + ["up", "-d", "mariadb"], check=True)

        log_info("Waiting for MariaDB (up to 180 seconds)...")
        took = wait_for_mariadb(dc, db_port, timeout=180)
        if took is None:
            log_error("MariaDB failed to start within 180 seconds")
            sys.exit(1)
        log_success(f"MariaDB ready on port {db_port} (took {took}s)")
        time.sleep(2)
    else:
        log_warn("Skipping infrastructure startup")

    # Step 3: Run Go benchmark
    log_info("Starting scalability benchmark...")
    print()

    result = subprocess.run([
        str(BENCHMARK_BIN),
        "--db-host", "localhost",
        "--db-port", str(db_port),
        "--db-name", "metalog_metastore",
        "--db-user", "root",
        "--db-password", "password",
        "--nodes", args.nodes,
        "--workers-per-node", args.workers_per_node,
        "--batch-multiplier", str(args.batch_multiplier),
        "--tasks-per-worker", str(args.tasks),
        "--tables", str(args.tables),
        "--pool-size", "200",
    ])

    print()
    if result.returncode == 0:
        log_success("Benchmark complete!")
    else:
        log_error(f"Benchmark failed with exit code {result.returncode}")

    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
