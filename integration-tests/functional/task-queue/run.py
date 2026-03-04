#!/usr/bin/env python3
"""
Task Queue Functional Tests.

Runs TaskQueueTest and TaskPrefetcherTest against a Testcontainers-managed
MariaDB instance. Verifies the full task queue state machine, concurrent
claiming, stale reclaim, dead-letter promotion, and TaskPrefetcher lifecycle.

Usage:
  ./integration-tests/functional/task-queue/run.py [options]

Options:
  -s, --skip-build   Skip Maven build (use existing JAR/classes)
"""
import argparse
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR  = Path(__file__).parent.resolve()
PROJECT_DIR = SCRIPT_DIR.parent.parent.parent.resolve()

RED    = "\033[0;31m"
GREEN  = "\033[0;32m"
BLUE   = "\033[0;34m"
BOLD   = "\033[1m"
NC     = "\033[0m"

def log_info(msg):    print(f"{BLUE}[INFO]{NC} {msg}", flush=True)
def log_success(msg): print(f"{GREEN}[SUCCESS]{NC} {msg}", flush=True)
def log_error(msg):   print(f"{RED}[ERROR]{NC} {msg}", file=sys.stderr, flush=True)


def main():
    parser = argparse.ArgumentParser(description="Task queue functional tests")
    parser.add_argument("-s", "--skip-build", action="store_true", help="Skip Maven build")
    args = parser.parse_args()

    settings_file = PROJECT_DIR / "settings-maven-central.xml"

    if not args.skip_build:
        log_info("Compiling...")
        cmd = ["mvn", "test-compile", "-q"]
        if settings_file.exists():
            cmd += ["--settings", str(settings_file)]
        result = subprocess.run(cmd, cwd=PROJECT_DIR)
        if result.returncode != 0:
            log_error("Compilation failed")
            sys.exit(result.returncode)
        log_success("Compilation complete")

    log_info("Running task queue functional tests (Testcontainers will start MariaDB)...")
    cmd = ["mvn", "test", "-Dtest=TaskQueueTest,TaskPrefetcherTest"]
    if settings_file.exists():
        cmd += ["--settings", str(settings_file)]
    result = subprocess.run(cmd, cwd=PROJECT_DIR)

    print()
    if result.returncode == 0:
        log_success("All tests passed")
    else:
        log_error(f"Tests failed (exit code {result.returncode})")

    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
