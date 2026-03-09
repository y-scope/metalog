# Reuse the main Go multi-stage Dockerfile
FROM golang:1.25-bookworm AS build
WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -o /out/metalog ./cmd/metalog

FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates librdkafka1 curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/metalog /app/metalog

ENTRYPOINT ["/app/metalog", "serve"]
CMD ["--config", "/etc/clp/node.yaml"]
