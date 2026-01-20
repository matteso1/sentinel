package storage

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// WAL (Write-Ahead Log) provides durability for memtable writes.
// Every write is appended to the WAL before being applied to the memtable.
// On crash recovery, the WAL is replayed to restore memtable state.
//
// Entry format:
//   - CRC32 checksum (4 bytes)
//   - Key length (4 bytes, varint)
//   - Value length (4 bytes, varint, -1 for tombstone)
//   - Timestamp (8 bytes)
//   - Key (variable)
//   - Value (variable, empty for tombstone)
type WAL struct {
	file     *os.File
	writer   *bufio.Writer
	path     string
	mu       sync.Mutex
	size     int64
	syncMode SyncMode
}

// SyncMode determines when WAL writes are synced to disk.
type SyncMode int

const (
	// SyncNone - no explicit sync (fastest, least durable)
	SyncNone SyncMode = iota
	// SyncBatch - sync periodically or on batch boundaries
	SyncBatch
	// SyncAlways - fsync after every write (slowest, most durable)
	SyncAlways
)

// WALConfig configures WAL behavior.
type WALConfig struct {
	SyncMode SyncMode
}

// DefaultWALConfig returns sensible defaults.
func DefaultWALConfig() WALConfig {
	return WALConfig{
		SyncMode: SyncBatch,
	}
}

// OpenWAL opens or creates a WAL file.
func OpenWAL(path string, config WALConfig) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &WAL{
		file:     file,
		writer:   bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		path:     path,
		size:     info.Size(),
		syncMode: config.SyncMode,
	}, nil
}

// Append writes an entry to the WAL.
func (w *WAL) Append(entry *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Build the entry data
	data := w.encodeEntry(entry)

	// Calculate CRC32 checksum
	checksum := crc32.ChecksumIEEE(data)

	// Write checksum
	if err := binary.Write(w.writer, binary.LittleEndian, checksum); err != nil {
		return err
	}

	// Write length-prefixed data
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}

	// Write data
	if _, err := w.writer.Write(data); err != nil {
		return err
	}

	w.size += int64(8 + len(data)) // 4 bytes checksum + 4 bytes length + data

	// Sync if needed
	if w.syncMode == SyncAlways {
		return w.sync()
	}

	return nil
}

// encodeEntry serializes an entry to bytes.
func (w *WAL) encodeEntry(entry *Entry) []byte {
	// Calculate size
	valueLen := len(entry.Value)
	if entry.Deleted {
		valueLen = -1
	}

	size := 8 + 4 + 4 + len(entry.Key) // timestamp + keyLen + valueLen + key
	if !entry.Deleted {
		size += len(entry.Value)
	}

	buf := make([]byte, size)
	offset := 0

	// Timestamp
	binary.LittleEndian.PutUint64(buf[offset:], entry.Timestamp)
	offset += 8

	// Key length
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(entry.Key)))
	offset += 4

	// Value length (-1 for tombstone)
	binary.LittleEndian.PutUint32(buf[offset:], uint32(valueLen))
	offset += 4

	// Key
	copy(buf[offset:], entry.Key)
	offset += len(entry.Key)

	// Value (if not deleted)
	if !entry.Deleted {
		copy(buf[offset:], entry.Value)
	}

	return buf
}

// Sync flushes and syncs the WAL to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sync()
}

func (w *WAL) sync() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Size returns the current WAL file size.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Delete removes the WAL file (called after successful flush to SSTable).
func (w *WAL) Delete() error {
	if err := w.Close(); err != nil {
		return err
	}
	return os.Remove(w.path)
}

// Recover reads all entries from the WAL for replay.
func RecoverWAL(path string) ([]*Entry, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No WAL to recover
		}
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var entries []*Entry

	for {
		// Read checksum
		var checksum uint32
		if err := binary.Read(reader, binary.LittleEndian, &checksum); err != nil {
			if err == io.EOF {
				break
			}
			return entries, err
		}

		// Read length
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			return entries, err
		}

		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return entries, err
		}

		// Verify checksum
		if crc32.ChecksumIEEE(data) != checksum {
			return entries, ErrCorruptedWAL
		}

		// Decode entry
		entry, err := decodeWALEntry(data)
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func decodeWALEntry(data []byte) (*Entry, error) {
	if len(data) < 16 {
		return nil, ErrCorruptedWAL
	}

	offset := 0

	// Timestamp
	timestamp := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// Key length
	keyLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Value length
	valueLen := int32(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	if len(data) < offset+int(keyLen) {
		return nil, ErrCorruptedWAL
	}

	// Key
	key := make([]byte, keyLen)
	copy(key, data[offset:offset+int(keyLen)])
	offset += int(keyLen)

	// Value
	var value []byte
	deleted := valueLen < 0
	if !deleted {
		if len(data) < offset+int(valueLen) {
			return nil, ErrCorruptedWAL
		}
		value = make([]byte, valueLen)
		copy(value, data[offset:offset+int(valueLen)])
	}

	return &Entry{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Deleted:   deleted,
	}, nil
}
