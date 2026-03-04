#!/usr/bin/env python3
"""
Task Queue Scalability Benchmark.

Sweeps worker counts × batch sizes to find the throughput ceiling of the task queue.
Useful for capacity planning: how many workers are needed for a given workload.

Usage:
  ./benchmarks/scalability/run.py [options]

Options:
  -w, --workers <csv>    Comma-separated worker counts (default: 1,25,50,75,100,125)
  -b, --batch <csv>      Comma-separated batch sizes (default: 1,5,10)
  -t, --tasks <n>        Tasks per worker (default: 50)
  -T, --tables <n>       Number of tables (default: 10)
  -J, --jitter <ms>      Deadlock retry jitter cap in ms (default: 50)
  -s, --skip-infra       Skip infrastructure startup (reuse existing containers)
"""
import argparse
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
BOLD   = "\033[1m"
NC     = "\033[0m"

def log_info(msg):    print(f"{BLUE}[INFO]{NC} {msg}", flush=True)
def log_success(msg): print(f"{GREEN}[SUCCESS]{NC} {msg}", flush=True)
def log_warn(msg):    print(f"{YELLOW}[WARN]{NC} {msg}", flush=True)
def log_error(msg):   print(f"{RED}[ERROR]{NC} {msg}", file=sys.stderr, flush=True)

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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Task queue scalability benchmark")
    parser.add_argument("-w", "--workers",    default="1,25,50,75,100,125", help="Worker counts CSV")
    parser.add_argument("-b", "--batch",      default="1,5,10",             help="Batch sizes CSV")
    parser.add_argument("-t", "--tasks",      type=int, default=50,         help="Tasks per worker (default: 50)")
    parser.add_argument("-T", "--tables",     type=int, default=10,         help="Number of tables (default: 10)")
    parser.add_argument("-J", "--jitter",     type=int, default=50,         help="Deadlock retry jitter cap in ms (default: 50)")
    parser.add_argument("-s", "--skip-infra", action="store_true",          help="Skip infrastructure startup")
    args = parser.parse_args()

    load_env()
    db_port = int(os.environ.get("DB_PORT", 3306))
    dc = find_docker_compose()

    worker_list = args.workers.split(",")
    batch_list  = args.batch.split(",")
    num_tests   = len(worker_list) * len(batch_list)

    print()
    print(f"{BOLD}╔════════════════════════════════════════════════════════════════════╗")
    print( "║           TASK QUEUE SCALABILITY BENCHMARK                         ║")
    print(f"╚════════════════════════════════════════════════════════════════════╝{NC}")
    print()
    print("Configuration:")
    print(f"  Workers:          {args.workers}")
    print(f"  Batch sizes:      {args.batch}")
    print(f"  Tasks per worker: {args.tasks}")
    print(f"  Tables:           {args.tables}")
    print(f"  Jitter cap:       {args.jitter} ms")
    print(f"  DB port:          {db_port}")
    print(f"  Skip infra:       {args.skip_infra}")
    print()
    print(f"{YELLOW}Running {num_tests} test configurations...{NC}")
    print()

    # Step 1: Build
    build_jar()

    # Step 2: Start MariaDB
    if not args.skip_infra:
        log_info("Stopping existing containers...")
        subprocess.run(dc + ["down", "-v", "--remove-orphans"], capture_output=True)
        subprocess.run(
            ["docker", "rm", "-f", "metalog-mariadb-scalability", "metalog-mariadb", "metalog-mariadb-task-queue"],
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
        log_success("Infrastructure ready")
    else:
        log_warn("Skipping infrastructure startup")

    # Step 3: Run benchmark
    log_info("Starting scalability benchmark (this may take several minutes)...")
    print()

    env = os.environ.copy()
    env.update({
        "DB_HOST":     "localhost",
        "DB_PORT":     str(db_port),
        "DB_NAME":     "metalog_metastore",
        "DB_USER":     "root",
        "DB_PASSWORD": "password",
    })

    result = subprocess.run(
        [
            "java",
            f"-Dworkers={args.workers}",
            f"-DbatchSizes={args.batch}",
            f"-DtasksPerWorker={args.tasks}",
            f"-Dtables={args.tables}",
            f"-DjitterMaxMs={args.jitter}",
            "-cp", CLASSPATH,
            "com.yscope.clp.service.tools.benchmark.TaskQueueScalabilityBenchmark",
        ],
        env=env,
    )

    print()
    if result.returncode == 0:
        log_success("Benchmark complete!")
    else:
        log_error(f"Benchmark failed with exit code {result.returncode}")

    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
