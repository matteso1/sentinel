package storage

import (
	"sync"
	"sync/atomic"
	"time"
)

// MemTable is an in-memory data structure that buffers writes before flushing to disk.
// It wraps a skip list and tracks size for flush threshold decisions.
type MemTable struct {
	sl         *SkipList
	id         uint64        // Unique ID for this memtable
	createdAt  time.Time     // When this memtable was created
	frozen     atomic.Bool   // Whether this memtable is frozen (no more writes)
	mu         sync.RWMutex  // Protects concurrent access
}

// MemTableConfig configures memtable behavior.
type MemTableConfig struct {
	MaxSize int64 // Flush threshold in bytes (default 64MB)
}

// DefaultMemTableConfig returns sensible defaults.
func DefaultMemTableConfig() MemTableConfig {
	return MemTableConfig{
		MaxSize: 64 * 1024 * 1024, // 64MB
	}
}

var memtableIDCounter uint64

// NewMemTable creates a new memtable.
func NewMemTable() *MemTable {
	return &MemTable{
		sl:        NewSkipList(),
		id:        atomic.AddUint64(&memtableIDCounter, 1),
		createdAt: time.Now(),
	}
}

// Put inserts or updates a key-value pair.
// Returns an error if the memtable is frozen.
func (m *MemTable) Put(key, value []byte) error {
	if m.frozen.Load() {
		return ErrMemTableFrozen
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	
	timestamp := uint64(time.Now().UnixNano())
	m.sl.Put(key, value, timestamp)
	return nil
}

// Get retrieves a value by key.
// Returns (value, found). If found is false, key doesn't exist or was deleted.
func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	value, _, found := m.sl.Get(key)
	return value, found
}

// Delete marks a key as deleted (tombstone).
func (m *MemTable) Delete(key []byte) error {
	if m.frozen.Load() {
		return ErrMemTableFrozen
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	
	timestamp := uint64(time.Now().UnixNano())
	m.sl.Delete(key, timestamp)
	return nil
}

// Size returns the approximate memory usage in bytes.
func (m *MemTable) Size() int64 {
	return m.sl.Size()
}

// Count returns the number of entries (including tombstones).
func (m *MemTable) Count() int64 {
	return m.sl.Count()
}

// ID returns the unique identifier for this memtable.
func (m *MemTable) ID() uint64 {
	return m.id
}

// Freeze marks the memtable as immutable. No more writes allowed.
func (m *MemTable) Freeze() {
	m.frozen.Store(true)
}

// IsFrozen returns whether the memtable is frozen.
func (m *MemTable) IsFrozen() bool {
	return m.frozen.Load()
}

// ShouldFlush returns true if the memtable exceeds the given size threshold.
func (m *MemTable) ShouldFlush(maxSize int64) bool {
	return m.Size() >= maxSize
}

// NewIterator returns an iterator over all entries in sorted order.
func (m *MemTable) NewIterator() *Iterator {
	return m.sl.NewIterator()
}

// Entries returns all entries as a slice (for flushing to SSTable).
// The memtable should be frozen before calling this.
func (m *MemTable) Entries() []*Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	entries := make([]*Entry, 0, m.sl.Count())
	iter := m.sl.NewIterator()
	defer iter.Close()
	
	for iter.Next() {
		entries = append(entries, iter.Entry())
	}
	return entries
}
