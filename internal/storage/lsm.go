package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LSM is the main Log-Structured Merge tree storage engine.
// It coordinates memtables, SSTables, WAL, and compaction.
type LSM struct {
	// Active memtable for writes
	memtable *MemTable
	// Immutable memtables waiting to be flushed
	immutableMemtables []*MemTable
	// Sorted SSTables by level (L0, L1, L2, ...)
	levels [][]*SSTable
	// WAL for durability
	wal *WAL
	// Configuration
	config LSMConfig
	// Data directory
	dataDir string
	// Synchronization
	mu sync.RWMutex
	// SSTable ID counter
	sstableCounter uint64
	// Background workers
	flushChan chan struct{}
	closeChan chan struct{}
	wg        sync.WaitGroup
}

// LSMConfig configures the LSM tree behavior.
type LSMConfig struct {
	// MemTableSize is the size threshold for flushing memtable to disk.
	MemTableSize int64
	// MaxLevels is the maximum number of SSTable levels.
	MaxLevels int
	// Level0CompactionTrigger is the number of L0 files that trigger compaction.
	Level0CompactionTrigger int
	// LevelSizeMultiplier is the size ratio between adjacent levels.
	LevelSizeMultiplier int
	// WALSyncMode determines when WAL is synced to disk.
	WALSyncMode SyncMode
}

// DefaultLSMConfig returns production-ready defaults.
func DefaultLSMConfig() LSMConfig {
	return LSMConfig{
		MemTableSize:            64 * 1024 * 1024, // 64MB
		MaxLevels:               7,
		Level0CompactionTrigger: 4,
		LevelSizeMultiplier:     10,
		WALSyncMode:             SyncBatch,
	}
}

// Open creates or opens an LSM tree at the given directory.
func Open(dataDir string, config LSMConfig) (*LSM, error) {
	// Create data directory
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	
	// Initialize levels
	levels := make([][]*SSTable, config.MaxLevels)
	for i := range levels {
		levels[i] = make([]*SSTable, 0)
	}
	
	// Open WAL
	walPath := filepath.Join(dataDir, "wal.log")
	wal, err := OpenWAL(walPath, WALConfig{SyncMode: config.WALSyncMode})
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	
	lsm := &LSM{
		memtable:           NewMemTable(),
		immutableMemtables: make([]*MemTable, 0),
		levels:             levels,
		wal:                wal,
		config:             config,
		dataDir:            dataDir,
		flushChan:          make(chan struct{}, 1),
		closeChan:          make(chan struct{}),
	}
	
	// Recover from WAL
	if err := lsm.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}
	
	// Load existing SSTables
	if err := lsm.loadSSTables(); err != nil {
		return nil, fmt.Errorf("failed to load SSTables: %w", err)
	}
	
	// Start background flush worker
	lsm.wg.Add(1)
	go lsm.flushWorker()
	
	return lsm, nil
}

func (l *LSM) recover() error {
	walPath := filepath.Join(l.dataDir, "wal.log")
	entries, err := RecoverWAL(walPath)
	if err != nil {
		return err
	}
	
	for _, entry := range entries {
		if entry.Deleted {
			l.memtable.Delete(entry.Key)
		} else {
			l.memtable.Put(entry.Key, entry.Value)
		}
	}
	
	return nil
}

func (l *LSM) loadSSTables() error {
	// Scan data directory for SSTable files
	pattern := filepath.Join(l.dataDir, "*.sst")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	
	for _, path := range files {
		sst, err := OpenSSTable(path)
		if err != nil {
			// Log and skip corrupted files
			continue
		}
		// For now, put all in L0 (proper level detection would parse filename)
		l.levels[0] = append(l.levels[0], sst)
	}
	
	return nil
}

// Put inserts or updates a key-value pair.
func (l *LSM) Put(key, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	// Write to WAL first (durability)
	entry := &Entry{
		Key:       key,
		Value:     value,
		Timestamp: uint64(time.Now().UnixNano()),
		Deleted:   false,
	}
	if err := l.wal.Append(entry); err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}
	
	// Write to memtable
	if err := l.memtable.Put(key, value); err != nil {
		return err
	}
	
	// Check if memtable needs to be flushed
	if l.memtable.ShouldFlush(l.config.MemTableSize) {
		l.triggerFlush()
	}
	
	return nil
}

// Get retrieves a value by key.
func (l *LSM) Get(key []byte) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	// 1. Check active memtable
	if value, found := l.memtable.Get(key); found {
		return value, nil
	}
	
	// 2. Check immutable memtables (newest first)
	for i := len(l.immutableMemtables) - 1; i >= 0; i-- {
		if value, found := l.immutableMemtables[i].Get(key); found {
			return value, nil
		}
	}
	
	// 3. Check SSTables level by level
	for level := 0; level < len(l.levels); level++ {
		sstables := l.levels[level]
		
		// L0: check all files (may overlap)
		if level == 0 {
			// Check newest first
			for i := len(sstables) - 1; i >= 0; i-- {
				if !sstables[i].Contains(key) {
					continue
				}
				if value, found := sstables[i].Get(key); found {
					return value, nil
				}
			}
		} else {
			// L1+: binary search (non-overlapping)
			idx := sort.Search(len(sstables), func(i int) bool {
				return sstables[i].Contains(key)
			})
			if idx < len(sstables) && sstables[idx].Contains(key) {
				if value, found := sstables[idx].Get(key); found {
					return value, nil
				}
			}
		}
	}
	
	return nil, ErrKeyNotFound
}

// Delete marks a key as deleted.
func (l *LSM) Delete(key []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	// Write tombstone to WAL
	entry := &Entry{
		Key:       key,
		Timestamp: uint64(time.Now().UnixNano()),
		Deleted:   true,
	}
	if err := l.wal.Append(entry); err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}
	
	// Write to memtable
	return l.memtable.Delete(key)
}

func (l *LSM) triggerFlush() {
	// Freeze current memtable
	l.memtable.Freeze()
	l.immutableMemtables = append(l.immutableMemtables, l.memtable)
	
	// Create new active memtable
	l.memtable = NewMemTable()
	
	// Signal flush worker
	select {
	case l.flushChan <- struct{}{}:
	default:
	}
}

func (l *LSM) flushWorker() {
	defer l.wg.Done()
	
	for {
		select {
		case <-l.closeChan:
			return
		case <-l.flushChan:
			l.doFlush()
		}
	}
}

func (l *LSM) doFlush() {
	l.mu.Lock()
	if len(l.immutableMemtables) == 0 {
		l.mu.Unlock()
		return
	}
	
	// Take the oldest immutable memtable
	memtable := l.immutableMemtables[0]
	l.immutableMemtables = l.immutableMemtables[1:]
	l.mu.Unlock()
	
	// Generate SSTable filename
	id := atomic.AddUint64(&l.sstableCounter, 1)
	sstPath := filepath.Join(l.dataDir, fmt.Sprintf("L0_%d.sst", id))
	
	// Write SSTable
	writer, err := NewSSTableWriter(sstPath)
	if err != nil {
		return // Log error in production
	}
	
	entries := memtable.Entries()
	for _, entry := range entries {
		writer.Add(entry)
	}
	
	if err := writer.Finish(); err != nil {
		return // Log error in production
	}
	
	// Open the new SSTable
	sst, err := OpenSSTable(sstPath)
	if err != nil {
		return
	}
	
	// Add to L0
	l.mu.Lock()
	l.levels[0] = append(l.levels[0], sst)
	l.mu.Unlock()
	
	// Check if compaction needed
	if len(l.levels[0]) >= l.config.Level0CompactionTrigger {
		l.triggerCompaction()
	}
}

func (l *LSM) triggerCompaction() {
	// TODO: Implement compaction in Phase 1.5
	// This would merge L0 SSTables into L1, then cascade down
}

// Close gracefully shuts down the LSM tree.
func (l *LSM) Close() error {
	close(l.closeChan)
	l.wg.Wait()
	
	// Flush remaining memtable
	if l.memtable.Count() > 0 {
		l.triggerFlush()
		l.doFlush()
	}
	
	// Close all SSTables
	for _, level := range l.levels {
		for _, sst := range level {
			sst.Close()
		}
	}
	
	// Close WAL
	return l.wal.Close()
}

// Stats returns current statistics.
func (l *LSM) Stats() LSMStats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	levelCounts := make([]int, len(l.levels))
	totalSSTables := 0
	for i, level := range l.levels {
		levelCounts[i] = len(level)
		totalSSTables += len(level)
	}
	
	return LSMStats{
		MemTableSize:       l.memtable.Size(),
		MemTableCount:      l.memtable.Count(),
		ImmutableCount:     len(l.immutableMemtables),
		SSTableCount:       totalSSTables,
		LevelCounts:        levelCounts,
	}
}

// LSMStats contains runtime statistics.
type LSMStats struct {
	MemTableSize   int64
	MemTableCount  int64
	ImmutableCount int
	SSTableCount   int
	LevelCounts    []int
}
