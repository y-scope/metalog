FROM rust:1.98.0-bookworm AS build
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY proto/ proto/
COPY benches/ benches/
COPY tests/ tests/
RUN cargo build --locked --release --bin metalog

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 metalog && useradd --uid 10001 --gid metalog --no-create-home metalog
WORKDIR /app
COPY --from=build /build/target/release/metalog /app/metalog
USER 10001:10001
EXPOSE 9090 8081
STOPSIGNAL SIGTERM
ENTRYPOINT ["/app/metalog", "serve"]
CMD ["--config", "/etc/clp/node.yaml"]
