// Package storage implements a Log-Structured Merge (LSM) tree storage engine.
//
// The LSM-tree is optimized for high write throughput by buffering writes in an
// in-memory memtable before flushing to disk as immutable SSTables. Background
// compaction merges SSTables to reclaim space and improve read performance.
//
// Architecture:
//
//	┌─────────────────────────────────────────────────────────────────┐
//	│                        LSM-Tree                                  │
//	├─────────────────────────────────────────────────────────────────┤
//	│  Write Path:  Client → WAL → MemTable → (flush) → SSTable L0   │
//	│  Read Path:   Client → MemTable → L0 → L1 → ... → Ln           │
//	├─────────────────────────────────────────────────────────────────┤
//	│  Compaction:  L0 → L1 → L2 → ... (exponential size ratio)      │
//	└─────────────────────────────────────────────────────────────────┘
//
// Key components:
//   - MemTable: In-memory skip list for fast writes and range scans
//   - WAL: Write-ahead log for durability before memtable flush
//   - SSTable: Sorted String Table - immutable on-disk sorted key-value store
//   - Compaction: Background merge of SSTables to reduce read amplification
package storage
