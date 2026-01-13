package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestSkipList_BasicOperations(t *testing.T) {
	sl := NewSkipList()

	// Test Put
	sl.Put([]byte("key1"), []byte("value1"), 1)
	sl.Put([]byte("key2"), []byte("value2"), 2)
	sl.Put([]byte("key3"), []byte("value3"), 3)

	// Test Get
	if value, _, found := sl.Get([]byte("key1")); !found || string(value) != "value1" {
		t.Errorf("expected value1, got %s, found=%v", value, found)
	}
	if value, _, found := sl.Get([]byte("key2")); !found || string(value) != "value2" {
		t.Errorf("expected value2, got %s, found=%v", value, found)
	}

	// Test missing key
	if _, _, found := sl.Get([]byte("missing")); found {
		t.Error("expected not found for missing key")
	}

	// Test Delete (tombstone)
	sl.Delete([]byte("key2"), 4)
	if _, _, found := sl.Get([]byte("key2")); found {
		t.Error("expected not found after delete")
	}

	// Test Update
	sl.Put([]byte("key1"), []byte("updated"), 5)
	if value, _, found := sl.Get([]byte("key1")); !found || string(value) != "updated" {
		t.Errorf("expected updated, got %s", value)
	}
}

func TestSkipList_Iterator(t *testing.T) {
	sl := NewSkipList()

	// Insert in random order
	sl.Put([]byte("c"), []byte("3"), 1)
	sl.Put([]byte("a"), []byte("1"), 2)
	sl.Put([]byte("b"), []byte("2"), 3)

	// Iterate and check sorted order
	iter := sl.NewIterator()
	defer iter.Close()

	expected := []string{"a", "b", "c"}
	i := 0
	for iter.Next() {
		if string(iter.Entry().Key) != expected[i] {
			t.Errorf("expected %s at position %d, got %s", expected[i], i, string(iter.Entry().Key))
		}
		i++
	}

	if i != 3 {
		t.Errorf("expected 3 entries, got %d", i)
	}
}

func TestMemTable_BasicOperations(t *testing.T) {
	mt := NewMemTable()

	// Test Put/Get
	mt.Put([]byte("foo"), []byte("bar"))
	if value, found := mt.Get([]byte("foo")); !found || string(value) != "bar" {
		t.Errorf("expected bar, got %s", value)
	}

	// Test Delete
	mt.Delete([]byte("foo"))
	if _, found := mt.Get([]byte("foo")); found {
		t.Error("expected not found after delete")
	}

	// Test freeze
	mt.Put([]byte("key"), []byte("value"))
	mt.Freeze()

	if err := mt.Put([]byte("newkey"), []byte("value")); err != ErrMemTableFrozen {
		t.Errorf("expected ErrMemTableFrozen, got %v", err)
	}
}

func TestLSM_BasicOperations(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "sentinel-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Open LSM
	config := DefaultLSMConfig()
	config.MemTableSize = 1024 // Small size for testing

	lsm, err := Open(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	// Test Put/Get
	if err := lsm.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}

	value, err := lsm.Get([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "world" {
		t.Errorf("expected world, got %s", value)
	}

	// Test Delete
	if err := lsm.Delete([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	if _, err := lsm.Get([]byte("hello")); err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}

	// Close
	if err := lsm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLSM_LargeWorkload(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-bench-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := DefaultLSMConfig()
	config.MemTableSize = 1024 * 1024 // 1MB - large enough to hold all test data in memory

	lsm, err := Open(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	// Write many entries
	n := 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := lsm.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}

	// Read them back
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		expectedValue := fmt.Sprintf("value%06d", i)

		value, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("failed to get key %s: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Errorf("expected %s, got %s", expectedValue, value)
		}
	}

	// Print stats
	stats := lsm.Stats()
	t.Logf("Stats: memtable=%d bytes, sstables=%d, levels=%v",
		stats.MemTableSize, stats.SSTableCount, stats.LevelCounts)
}

func BenchmarkSkipList_Put(b *testing.B) {
	sl := NewSkipList()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		value := []byte(fmt.Sprintf("value%010d", i))
		sl.Put(key, value, uint64(i))
	}
}

func BenchmarkSkipList_Get(b *testing.B) {
	sl := NewSkipList()

	// Pre-populate
	n := 100000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		value := []byte(fmt.Sprintf("value%010d", i))
		sl.Put(key, value, uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%010d", i%n))
		sl.Get(key)
	}
}

func TestWAL_WriteAndRecover(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-wal-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	walPath := dir + "/test.wal"

	// Write entries to WAL
	{
		config := DefaultWALConfig()
		config.SyncMode = SyncAlways
		wal, err := OpenWAL(walPath, config)
		if err != nil {
			t.Fatal(err)
		}

		entries := []*Entry{
			{Key: []byte("key1"), Value: []byte("value1"), Timestamp: 1},
			{Key: []byte("key2"), Value: []byte("value2"), Timestamp: 2},
			{Key: []byte("key3"), Value: nil, Deleted: true, Timestamp: 3},
		}

		for _, entry := range entries {
			if err := wal.Append(entry); err != nil {
				t.Fatal(err)
			}
		}

		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Recover and verify
	{
		recovered, err := RecoverWAL(walPath)
		if err != nil {
			t.Fatal(err)
		}

		if len(recovered) != 3 {
			t.Errorf("expected 3 entries, got %d", len(recovered))
		}

		if string(recovered[0].Key) != "key1" || string(recovered[0].Value) != "value1" {
			t.Error("first entry mismatch")
		}

		if !recovered[2].Deleted {
			t.Error("expected third entry to be a tombstone")
		}
	}
}

func TestSSTable_WriteAndRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-sst-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	sstPath := dir + "/test.sst"

	// Write SSTable
	{
		writer, err := NewSSTableWriter(sstPath)
		if err != nil {
			t.Fatal(err)
		}

		// Add entries in sorted order
		entries := []*Entry{
			{Key: []byte("apple"), Value: []byte("red"), Timestamp: 1},
			{Key: []byte("banana"), Value: []byte("yellow"), Timestamp: 2},
			{Key: []byte("cherry"), Value: []byte("red"), Timestamp: 3},
		}

		for _, entry := range entries {
			if err := writer.Add(entry); err != nil {
				t.Fatal(err)
			}
		}

		if err := writer.Finish(); err != nil {
			t.Fatal(err)
		}
	}

	// Read SSTable
	{
		sst, err := OpenSSTable(sstPath)
		if err != nil {
			t.Fatal(err)
		}
		defer sst.Close()

		// Test Get
		if value, found := sst.Get([]byte("banana")); !found || string(value) != "yellow" {
			t.Errorf("expected yellow, got %s, found=%v", value, found)
		}

		// Test missing key
		if _, found := sst.Get([]byte("grape")); found {
			t.Error("expected grape not to be found")
		}

		// Test Contains
		if !sst.Contains([]byte("apple")) {
			t.Error("expected SSTable to contain apple")
		}

		if sst.Contains([]byte("aaa")) {
			t.Error("expected SSTable to not contain aaa (before min key)")
		}

		// Note: EntryCount is not populated during read in current implementation
		// This could be enhanced to store/read entryCount from footer
	}
}

func TestLSM_ConcurrentAccess(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-concurrent-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := DefaultLSMConfig()
	config.MemTableSize = 1024 * 1024

	lsm, err := Open(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	// Concurrent writes and reads
	done := make(chan bool)
	errors := make(chan error, 10)

	// Writer goroutine
	go func() {
		for i := 0; i < 500; i++ {
			key := []byte(fmt.Sprintf("concurrent-key-%04d", i))
			value := []byte(fmt.Sprintf("concurrent-value-%04d", i))
			if err := lsm.Put(key, value); err != nil {
				errors <- err
				return
			}
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 500; i++ {
			key := []byte(fmt.Sprintf("concurrent-key-%04d", i))
			// It's OK if key not found - concurrent writes
			lsm.Get(key)
		}
		done <- true
	}()

	// Wait for both
	<-done
	<-done

	select {
	case err := <-errors:
		t.Errorf("concurrent access error: %v", err)
	default:
		// No errors
	}

	// Verify final state
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("concurrent-key-%04d", i))
		_, err := lsm.Get(key)
		if err != nil {
			t.Errorf("missing key after concurrent writes: %s", key)
		}
	}
}

func BenchmarkLSM_Put(b *testing.B) {
	dir, _ := os.MkdirTemp("", "sentinel-bench-*")
	defer os.RemoveAll(dir)

	lsm, _ := Open(dir, DefaultLSMConfig())
	defer lsm.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		value := []byte(fmt.Sprintf("value%010d", i))
		lsm.Put(key, value)
	}
}
