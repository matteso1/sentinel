# Contributing to Sentinel

Thank you for your interest in contributing to Sentinel! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Go 1.22 or later
- Protocol Buffers compiler (for modifying `.proto` files)
- Make (optional, for using Makefile commands)

### Getting Started

1. Fork the repository
2. Clone your fork:

   ```bash
   git clone https://github.com/YOUR_USERNAME/sentinel.git
   cd sentinel
   ```

3. Install dependencies:

   ```bash
   go mod download
   ```

4. Run tests to verify setup:

   ```bash
   go test ./...
   ```

## Code Style

- Follow standard Go formatting (`gofmt`)
- Use meaningful variable and function names
- Add comments for exported types and functions
- Keep functions focused and reasonably sized

### Formatting

Before submitting, ensure your code is formatted:

```bash
gofmt -w .
go vet ./...
```

## Testing

### Running Tests

```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./...

# Run with race detection
go test -race ./...

# Run specific package tests
go test ./internal/storage/...

# Run with coverage
go test -cover ./...
```

### Running Benchmarks

```bash
go test -bench=. ./internal/storage/...
```

### Writing Tests

- Place test files alongside the code they test (`foo_test.go` next to `foo.go`)
- Use table-driven tests where appropriate
- Test edge cases and error conditions
- Integration tests go in the root `integration_test.go`

## Pull Request Process

1. **Create a branch** for your changes:

   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** with clear, atomic commits

3. **Ensure all tests pass**:

   ```bash
   go test -race ./...
   ```

4. **Update documentation** if needed

5. **Push your branch** and create a Pull Request

6. **Describe your changes** in the PR description:
   - What problem does this solve?
   - How does it solve it?
   - Any breaking changes?

## Project Structure

```text
sentinel/
├── cmd/                    # Application entrypoints
│   ├── sentinel-server/    # Server binary
│   └── sentinel-cli/       # CLI tool
├── internal/               # Private packages
│   ├── storage/            # LSM-tree storage engine
│   ├── broker/             # Message broker logic
│   ├── raft/               # Raft consensus
│   ├── server/             # gRPC server
│   └── metrics/            # Prometheus metrics
├── proto/                  # Protocol buffer definitions
└── docs/                   # Documentation
```

## Regenerating Protocol Buffers

If you modify `proto/sentinel.proto`:

```bash
protoc --go_out=. --go-grpc_out=. proto/sentinel.proto
```

## Questions?

Feel free to open an issue for questions or discussions about contributing.
