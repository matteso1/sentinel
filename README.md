# Sentinel

A high-throughput distributed log streaming engine written in Go.

## Features

- **LSM-Tree Storage Engine**: Custom Log-Structured Merge tree with:
  - In-memory skip list memtable for O(log n) operations
  - Immutable SSTables with block-based format and CRC32 checksums
  - Write-ahead log (WAL) for crash recovery
  - Background flush worker for async persistence

- **High Performance Target**: 100k+ msg/s ingestion throughput

## Project Structure

```
sentinel/
├── cmd/
│   ├── sentinel-server/    # Server entrypoint (coming soon)
│   └── sentinel-cli/       # CLI client (coming soon)
├── internal/
│   └── storage/            # LSM-tree storage engine
│       ├── skiplist.go     # Skip list data structure
│       ├── memtable.go     # In-memory buffer
│       ├── wal.go          # Write-ahead log
│       ├── sstable.go      # Sorted String Table
│       └── lsm.go          # LSM coordinator
└── proto/                  # Protobuf definitions (coming soon)
```

## Building

```bash
go build ./...
```

## Testing

```bash
go test -v ./internal/storage/...
```

## Benchmarking

```bash
go test -bench=. ./internal/storage/...
```

## Roadmap

- [x] Phase 1: LSM Storage Engine
- [ ] Phase 2: gRPC Wire Protocol
- [ ] Phase 3: Broker & Partitioning
- [ ] Phase 4: Raft-inspired Replication
- [ ] Phase 5: CLI & Observability

## License

MIT
