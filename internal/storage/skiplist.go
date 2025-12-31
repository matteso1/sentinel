package storage

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// Entry represents a key-value pair with metadata.
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	Deleted   bool // Tombstone marker for deletions
}

// Compare returns -1, 0, or 1 if this entry's key is less than, equal to, or greater than other.
func (e *Entry) Compare(other *Entry) int {
	return bytes.Compare(e.Key, other.Key)
}

// Size returns the approximate memory footprint of this entry in bytes.
func (e *Entry) Size() int {
	return len(e.Key) + len(e.Value) + 17 // 8 bytes timestamp + 1 byte deleted flag + overhead
}

// skipListNode is a node in the skip list.
type skipListNode struct {
	entry   *Entry
	forward []*skipListNode
}

// SkipList is a probabilistic data structure for O(log n) operations.
// It's the backbone of the MemTable, providing fast inserts and ordered iteration.
type SkipList struct {
	head     *skipListNode
	maxLevel int
	level    int
	size     int64
	count    int64
	mu       sync.RWMutex
	rng      uint64 // Simple PRNG state for level generation
}

const (
	maxSkipListLevel = 16   // Maximum height of skip list
	skipListP        = 0.25 // Probability of level increase
)

// NewSkipList creates a new skip list.
func NewSkipList() *SkipList {
	return &SkipList{
		head: &skipListNode{
			forward: make([]*skipListNode, maxSkipListLevel),
		},
		maxLevel: maxSkipListLevel,
		level:    0,
		rng:      uint64(1), // Seed PRNG
	}
}

// randomLevel generates a random level for a new node using geometric distribution.
func (s *SkipList) randomLevel() int {
	level := 0
	// XorShift64 PRNG for fast random number generation
	s.rng ^= s.rng << 13
	s.rng ^= s.rng >> 7
	s.rng ^= s.rng << 17

	for level < s.maxLevel-1 && (s.rng&0xFFFF) < uint64(0xFFFF/4) {
		level++
		s.rng ^= s.rng << 13
		s.rng ^= s.rng >> 7
		s.rng ^= s.rng << 17
	}
	return level
}

// Put inserts or updates a key-value pair.
// Returns the size delta (positive for new entry, may be negative for smaller update).
func (s *SkipList) Put(key, value []byte, timestamp uint64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*skipListNode, s.maxLevel)
	current := s.head

	// Find insertion point
	for i := s.level; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].entry.Key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	// Check if key exists
	if current.forward[0] != nil && bytes.Equal(current.forward[0].entry.Key, key) {
		// Update existing entry
		oldSize := int64(current.forward[0].entry.Size())
		current.forward[0].entry.Value = value
		current.forward[0].entry.Timestamp = timestamp
		current.forward[0].entry.Deleted = false
		newSize := int64(current.forward[0].entry.Size())
		delta := newSize - oldSize
		atomic.AddInt64(&s.size, delta)
		return delta
	}

	// Insert new node
	level := s.randomLevel()
	if level > s.level {
		for i := s.level + 1; i <= level; i++ {
			update[i] = s.head
		}
		s.level = level
	}

	entry := &Entry{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Deleted:   false,
	}

	node := &skipListNode{
		entry:   entry,
		forward: make([]*skipListNode, level+1),
	}

	for i := 0; i <= level; i++ {
		node.forward[i] = update[i].forward[i]
		update[i].forward[i] = node
	}

	entrySize := int64(entry.Size())
	atomic.AddInt64(&s.size, entrySize)
	atomic.AddInt64(&s.count, 1)
	return entrySize
}

// Get retrieves the value for a key. Returns nil, false if not found or deleted.
func (s *SkipList) Get(key []byte) ([]byte, uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	current := s.head
	for i := s.level; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].entry.Key, key) < 0 {
			current = current.forward[i]
		}
	}

	current = current.forward[0]
	if current != nil && bytes.Equal(current.entry.Key, key) {
		if current.entry.Deleted {
			return nil, current.entry.Timestamp, false
		}
		return current.entry.Value, current.entry.Timestamp, true
	}
	return nil, 0, false
}

// Delete marks a key as deleted (tombstone).
func (s *SkipList) Delete(key []byte, timestamp uint64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*skipListNode, s.maxLevel)
	current := s.head

	for i := s.level; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].entry.Key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]
	if current != nil && bytes.Equal(current.entry.Key, key) {
		// Mark as deleted (tombstone)
		oldSize := int64(current.entry.Size())
		current.entry.Value = nil
		current.entry.Deleted = true
		current.entry.Timestamp = timestamp
		newSize := int64(current.entry.Size())
		delta := newSize - oldSize
		atomic.AddInt64(&s.size, delta)
		return delta
	}

	// Key doesn't exist, create tombstone
	level := s.randomLevel()
	if level > s.level {
		for i := s.level + 1; i <= level; i++ {
			update[i] = s.head
		}
		s.level = level
	}

	entry := &Entry{
		Key:       key,
		Value:     nil,
		Timestamp: timestamp,
		Deleted:   true,
	}

	node := &skipListNode{
		entry:   entry,
		forward: make([]*skipListNode, level+1),
	}

	for i := 0; i <= level; i++ {
		node.forward[i] = update[i].forward[i]
		update[i].forward[i] = node
	}

	entrySize := int64(entry.Size())
	atomic.AddInt64(&s.size, entrySize)
	atomic.AddInt64(&s.count, 1)
	return entrySize
}

// Size returns the approximate memory usage in bytes.
func (s *SkipList) Size() int64 {
	return atomic.LoadInt64(&s.size)
}

// Count returns the number of entries (including tombstones).
func (s *SkipList) Count() int64 {
	return atomic.LoadInt64(&s.count)
}

// Iterator provides ordered iteration over skip list entries.
type Iterator struct {
	current *skipListNode
	sl      *SkipList
}

// NewIterator returns an iterator positioned before the first element.
func (s *SkipList) NewIterator() *Iterator {
	s.mu.RLock()
	return &Iterator{
		current: s.head,
		sl:      s,
	}
}

// Next advances the iterator. Returns false when exhausted.
func (it *Iterator) Next() bool {
	if it.current == nil {
		return false
	}
	it.current = it.current.forward[0]
	return it.current != nil
}

// Entry returns the current entry.
func (it *Iterator) Entry() *Entry {
	if it.current == nil {
		return nil
	}
	return it.current.entry
}

// Close releases the read lock.
func (it *Iterator) Close() {
	it.sl.mu.RUnlock()
}

// Seek positions the iterator at the first entry >= key.
func (it *Iterator) Seek(key []byte) bool {
	current := it.sl.head
	for i := it.sl.level; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].entry.Key, key) < 0 {
			current = current.forward[i]
		}
	}
	it.current = current.forward[0]
	return it.current != nil
}
