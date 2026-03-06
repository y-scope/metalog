.PHONY: all build test test-integration proto clean lint

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOVET=$(GOCMD) vet
BINARY_DIR=bin

# Proto parameters
PROTO_DIR=proto
GEN_DIR=gen/proto
PROTOC=protoc

all: proto build

# Build all binaries
build:
	$(GOBUILD) -o $(BINARY_DIR)/metalog-server ./cmd/metalog-server
	$(GOBUILD) -o $(BINARY_DIR)/metalog-worker ./cmd/metalog-worker
	$(GOBUILD) -o $(BINARY_DIR)/metalog-apiserver ./cmd/metalog-apiserver

# Generate protobuf/gRPC Go code
proto:
	$(PROTOC) \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR)/splitspb --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR)/splitspb --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/splits.proto
	$(PROTOC) \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR)/ingestionpb --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR)/ingestionpb --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/ingestion.proto
	$(PROTOC) \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR)/metadatapb --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR)/metadatapb --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/metadata.proto
	$(PROTOC) \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR)/coordinatorpb --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR)/coordinatorpb --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/coordinator.proto

# Run unit tests
test:
	$(GOTEST) ./internal/...

# Run integration tests (requires Docker)
test-integration:
	$(GOTEST) -tags=integration -timeout 300s ./internal/...

# Lint
lint:
	$(GOVET) ./...

# Clean build artifacts
clean:
	rm -rf $(BINARY_DIR) $(GEN_DIR)
