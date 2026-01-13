.PHONY: build test test-race test-cover bench clean run proto lint

# Build binaries
build:
	go build -o bin/sentinel-server ./cmd/sentinel-server
	go build -o bin/sentinel-cli ./cmd/sentinel-cli

# Run server
run: build
	./bin/sentinel-server --port 9092 --data ./data

# Run all tests
test:
	go test ./...

# Run tests with race detection
test-race:
	go test -race ./...

# Run tests with coverage
test-cover:
	go test -cover ./...
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run benchmarks
bench:
	go test -bench=. -benchmem ./internal/storage/...

# Run linter
lint:
	go vet ./...
	@if [ -n "$$(gofmt -l .)" ]; then \
		echo "Code needs formatting. Run 'make fmt'"; \
		gofmt -l .; \
		exit 1; \
	fi

# Format code
fmt:
	gofmt -w .

# Regenerate protobuf
proto:
	protoc --go_out=. --go-grpc_out=. proto/sentinel.proto

# Clean build artifacts
clean:
	rm -rf bin/
	rm -rf data/
	rm -f coverage.out coverage.html
	rm -f *.sst *.log

# Build Docker image
docker:
	docker build -t sentinel:latest .

# Run Docker container
docker-run: docker
	docker run -p 9092:9092 sentinel:latest

# Help
help:
	@echo "Sentinel Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make build       Build server and CLI binaries"
	@echo "  make run         Build and run the server"
	@echo "  make test        Run all tests"
	@echo "  make test-race   Run tests with race detection"
	@echo "  make test-cover  Run tests with coverage report"
	@echo "  make bench       Run benchmarks"
	@echo "  make lint        Run linters"
	@echo "  make fmt         Format code"
	@echo "  make proto       Regenerate protobuf files"
	@echo "  make clean       Remove build artifacts"
	@echo "  make docker      Build Docker image"
	@echo "  make docker-run  Run Docker container"
