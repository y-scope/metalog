.PHONY: all build test test-integration test-storage-image lint fmt clean

all: build

build:
	cargo build --locked --release --bin metalog

test:
	cargo test --locked --workspace --all-features --exclude metalog-integration-tests

test-storage-image:
	docker build -t metalog-minio-test:2025-10-15 -f docker/Dockerfile.minio-test docker

test-integration: test-storage-image
	cargo test --locked -p metalog-integration-tests -- --test-threads=4

lint:
	cargo clippy --locked --workspace --all-targets --all-features -- -D warnings

fmt:
	cargo +nightly-2026-08-30 fmt --all -- --check

clean:
	cargo clean
