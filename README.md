# metalog

Rust metadata catalog for [CLP](https://github.com/y-scope/clp), backed by MariaDB/MySQL.
This branch supports gRPC ingestion/query/admin, S3/MinIO storage, retention, and multi-node
coordinator HA. The retained Go sources provide migration reference; the default build and Docker
image run Rust.

Start with the [Rust deployment and migration guide](docs/operations/rust-production.md).
It describes acknowledgment semantics, retention safety, configuration, unsupported features, and
rollout requirements. Historical documents describe a broader Go feature set; they are not a Rust
feature checklist. Kafka, consolidation execution, aggregation/sketch ingestion, and telemetry
exporters are currently disabled explicitly.

## Build and run

Install Rust 1.98.0 and `protoc`, then:

```bash
make build
target/release/metalog serve --config docker/coordinator-node.yaml
```

The example configuration targets the local Docker network. For the complete local stack:

```bash
./docker/start.sh -d
# Optional second coordinator:
docker compose -f docker/docker-compose.yml up -d --scale coordinator-node=2
```

Register tables with `metalog admin register-table --addr HOST:PORT --table web_logs`.
Retention is opt-in; configure it explicitly using the migration guide.

## Development

```bash
make test
make test-integration                     # requires Docker
cargo test --locked -p metalog --test production_smoke
make lint
make fmt                                 # pinned nightly rustfmt
```

Core crates separate ingestion batching, schema allocation, SQL queries, storage, retention, HA,
and server lifecycle. Protobuf wire definitions live in `proto/`; Rust bindings are generated at
build time. Integration tests live in `tests/integration`, with server process tests in
`crates/metalog-bin/tests`.

## License

Apache-2.0
