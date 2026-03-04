# Reuse the main Go multi-stage Dockerfile
FROM golang:1.25-bookworm AS build
WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -o /out/metalog-server ./cmd/metalog-server \
    && CGO_ENABLED=1 go build -o /out/metalog-apiserver ./cmd/metalog-apiserver

FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates librdkafka1 curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/metalog-server /app/metalog-server
COPY --from=build /out/metalog-apiserver /app/metalog-apiserver

ENTRYPOINT ["/app/metalog-server"]
CMD ["--config", "/etc/clp/node.yaml"]
