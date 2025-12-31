package storage

import "errors"

var (
	// ErrMemTableFrozen is returned when attempting to write to a frozen memtable.
	ErrMemTableFrozen = errors.New("memtable is frozen")
	
	// ErrKeyNotFound is returned when a key doesn't exist.
	ErrKeyNotFound = errors.New("key not found")
	
	// ErrCorruptedWAL is returned when WAL data is corrupted.
	ErrCorruptedWAL = errors.New("corrupted WAL entry")
	
	// ErrCorruptedSSTable is returned when SSTable data is corrupted.
	ErrCorruptedSSTable = errors.New("corrupted SSTable")
)
