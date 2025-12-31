# Sentinel

A high-throughput distributed log streaming engine written in Go.

[![Tests](https://img.shields.io/badge/tests-21%20passing-brightgreen)]()
[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go)]()

## Features

- **LSM-Tree Storage Engine**: Custom Log-Structured Merge tree with skip list memtable, SSTables with CRC32 checksums, and write-ahead log for crash recovery

- **gRPC API**: Protobuf-defined Produce, Consume (streaming), and FetchMetadata RPCs

- **Topic/Partition Model**: Kafka-inspired message organization with hash/round-robin partitioning and consumer groups

- **Raft Consensus**: Leader election, log replication, and commit tracking for fault tolerance

- **Prometheus Metrics**: Built-in `/metrics` endpoint with counters, gauges, and latency tracking

## Quick Start

### Run the Server

```bash
go run ./cmd/sentinel-server --port 9092 --data ./data
```

### Use the CLI

```bash
# Produce messages
go run ./cmd/sentinel-cli produce -topic events -message "Hello, World!"

# Consume messages
go run ./cmd/sentinel-cli consume -topic events -from-beginning

# List topics
go run ./cmd/sentinel-cli topics -list
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Sentinel Cluster                         │
├─────────────────┬─────────────────┬─────────────────────────────┤
│    Broker 1     │    Broker 2     │         Broker 3            │
│   (Leader P0)   │  (Leader P1)    │        (Follower)           │
├─────────────────┴─────────────────┴─────────────────────────────┤
│                    gRPC / Protobuf Protocol                      │
├─────────────────┬─────────────────┬─────────────────────────────┤
│  Partition 0    │   Partition 1   │         Replicas            │
│  ┌───────────┐  │  ┌───────────┐  │                             │
│  │ LSM-Tree  │  │  │ LSM-Tree  │  │   Raft-inspired consensus   │
│  │ Storage   │  │  │ Storage   │  │   for leader election       │
│  └───────────┘  │  └───────────┘  │                             │
└─────────────────┴─────────────────┴─────────────────────────────┘
```

## Project Structure

```
sentinel/
├── cmd/
│   ├── sentinel-server/    # Server entrypoint
│   └── sentinel-cli/       # CLI client
├── internal/
│   ├── storage/            # LSM-tree storage engine
│   ├── broker/             # Topic/partition management
│   ├── raft/               # Consensus protocol
│   ├── server/             # gRPC server
│   └── metrics/            # Prometheus metrics
└── proto/                  # Protobuf definitions
```

## Testing

```bash
go test -v ./...
```

## Benchmarks

```bash
go test -bench=. ./internal/storage/...
```

Results on Intel i9-13900HX:

- **SkipList Put**: ~1.7 million ops/sec
- **SkipList Get**: ~3.9 million ops/sec

## Key Metrics (Resume Highlights)

- **100k+ msg/s** ingestion throughput target
- **Custom LSM-tree** storage engine from scratch
- **Raft-inspired** consensus for high availability
- **gRPC** for efficient service communication
- **5,000+ lines** of production-quality Go

## License

MIT
