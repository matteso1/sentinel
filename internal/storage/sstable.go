package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// SSTable (Sorted String Table) is an immutable on-disk data structure.
// It stores sorted key-value pairs with an index for fast lookups.
//
// File format:
//   ┌─────────────────────────────────────────────────────────────┐
//   │ Data Blocks                                                  │
//   │   Block 0: [Entry, Entry, Entry, ...]                       │
//   │   Block 1: [Entry, Entry, Entry, ...]                       │
//   │   ...                                                        │
//   ├─────────────────────────────────────────────────────────────┤
//   │ Index Block                                                  │
//   │   [firstKey, blockOffset, blockSize]                        │
//   │   [firstKey, blockOffset, blockSize]                        │
//   │   ...                                                        │
//   ├─────────────────────────────────────────────────────────────┤
//   │ Footer (fixed size)                                         │
//   │   indexOffset (8 bytes)                                     │
//   │   indexSize (8 bytes)                                       │
//   │   entryCount (8 bytes)                                      │
//   │   minKey (length-prefixed)                                  │
//   │   maxKey (length-prefixed)                                  │
//   │   checksum (4 bytes)                                        │
//   │   magic number (4 bytes)                                    │
//   └─────────────────────────────────────────────────────────────┘

const (
	sstableMagic     = 0x53535442 // "SSTB"
	sstableVersion   = 1
	blockSize        = 4 * 1024 // 4KB blocks
	bloomFilterFP    = 0.01     // 1% false positive rate
)

// SSTableWriter writes entries to an SSTable file.
type SSTableWriter struct {
	file        *os.File
	writer      *bufio.Writer
	path        string
	index       []indexEntry
	blockBuf    bytes.Buffer
	blockStart  int64
	entryCount  uint64
	minKey      []byte
	maxKey      []byte
	offset      int64
}

type indexEntry struct {
	firstKey    []byte
	blockOffset int64
	blockSize   int64
}

// NewSSTableWriter creates a writer for a new SSTable.
func NewSSTableWriter(path string) (*SSTableWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	
	return &SSTableWriter{
		file:   file,
		writer: bufio.NewWriterSize(file, 64*1024),
		path:   path,
		index:  make([]indexEntry, 0),
	}, nil
}

// Add writes an entry to the SSTable. Entries must be added in sorted order.
func (w *SSTableWriter) Add(entry *Entry) error {
	// Track min/max keys
	if w.entryCount == 0 {
		w.minKey = append([]byte{}, entry.Key...)
	}
	w.maxKey = append(w.maxKey[:0], entry.Key...)
	w.entryCount++
	
	// Start new block if needed
	if w.blockBuf.Len() == 0 {
		w.index = append(w.index, indexEntry{
			firstKey:    append([]byte{}, entry.Key...),
			blockOffset: w.offset,
		})
	}
	
	// Encode entry to block buffer
	w.encodeEntry(&w.blockBuf, entry)
	
	// Flush block if it exceeds block size
	if w.blockBuf.Len() >= blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	
	return nil
}

func (w *SSTableWriter) encodeEntry(buf *bytes.Buffer, entry *Entry) {
	// Key length (4 bytes)
	binary.Write(buf, binary.LittleEndian, uint32(len(entry.Key)))
	
	// Value length (-1 for tombstone) (4 bytes)
	valueLen := int32(len(entry.Value))
	if entry.Deleted {
		valueLen = -1
	}
	binary.Write(buf, binary.LittleEndian, valueLen)
	
	// Timestamp (8 bytes)
	binary.Write(buf, binary.LittleEndian, entry.Timestamp)
	
	// Key
	buf.Write(entry.Key)
	
	// Value (if not deleted)
	if !entry.Deleted {
		buf.Write(entry.Value)
	}
}

func (w *SSTableWriter) flushBlock() error {
	if w.blockBuf.Len() == 0 {
		return nil
	}
	
	// Calculate CRC
	data := w.blockBuf.Bytes()
	checksum := crc32.ChecksumIEEE(data)
	
	// Write block with CRC prefix
	if err := binary.Write(w.writer, binary.LittleEndian, checksum); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	if _, err := w.writer.Write(data); err != nil {
		return err
	}
	
	// Update index entry with block size
	blockTotalSize := int64(8 + len(data)) // 4 bytes CRC + 4 bytes length + data
	w.index[len(w.index)-1].blockSize = blockTotalSize
	w.offset += blockTotalSize
	
	// Clear block buffer
	w.blockBuf.Reset()
	
	return nil
}

// Finish completes the SSTable and writes the index and footer.
func (w *SSTableWriter) Finish() error {
	// Flush remaining block
	if err := w.flushBlock(); err != nil {
		return err
	}
	
	// Write index
	indexOffset := w.offset
	for _, idx := range w.index {
		// First key (length-prefixed)
		binary.Write(w.writer, binary.LittleEndian, uint32(len(idx.firstKey)))
		w.writer.Write(idx.firstKey)
		// Block offset
		binary.Write(w.writer, binary.LittleEndian, idx.blockOffset)
		// Block size
		binary.Write(w.writer, binary.LittleEndian, idx.blockSize)
		w.offset += int64(4 + len(idx.firstKey) + 16)
	}
	indexSize := w.offset - indexOffset
	
	// Write footer
	footer := bytes.Buffer{}
	binary.Write(&footer, binary.LittleEndian, indexOffset)
	binary.Write(&footer, binary.LittleEndian, indexSize)
	binary.Write(&footer, binary.LittleEndian, w.entryCount)
	
	// Min key
	binary.Write(&footer, binary.LittleEndian, uint32(len(w.minKey)))
	footer.Write(w.minKey)
	
	// Max key
	binary.Write(&footer, binary.LittleEndian, uint32(len(w.maxKey)))
	footer.Write(w.maxKey)
	
	// Footer checksum
	footerChecksum := crc32.ChecksumIEEE(footer.Bytes())
	binary.Write(&footer, binary.LittleEndian, footerChecksum)
	
	// Magic number
	binary.Write(&footer, binary.LittleEndian, uint32(sstableMagic))
	
	w.writer.Write(footer.Bytes())
	
	// Flush and close
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Path returns the file path.
func (w *SSTableWriter) Path() string {
	return w.path
}

// SSTable represents an open SSTable file for reading.
type SSTable struct {
	file       *os.File
	path       string
	index      []indexEntry
	minKey     []byte
	maxKey     []byte
	entryCount uint64
}

// OpenSSTable opens an existing SSTable for reading.
func OpenSSTable(path string) (*SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	
	// Read footer (last 4 bytes for magic)
	file.Seek(-4, io.SeekEnd)
	var magic uint32
	binary.Read(file, binary.LittleEndian, &magic)
	if magic != sstableMagic {
		file.Close()
		return nil, ErrCorruptedSSTable
	}
	
	// Seek back to read full footer
	// We need to know footer size, which starts with indexOffset
	file.Seek(-4-4, io.SeekEnd) // before checksum and magic
	var footerChecksum uint32
	binary.Read(file, binary.LittleEndian, &footerChecksum)
	
	// For simplicity, re-read from beginning of footer
	// First, get file size
	info, _ := file.Stat()
	fileSize := info.Size()
	
	// Read index offset from beginning of footer area
	// Footer format: indexOffset(8) + indexSize(8) + entryCount(8) + minKey + maxKey + checksum(4) + magic(4)
	// We'll read backwards to find footer start
	
	// Seek to read indexOffset, indexSize, entryCount
	file.Seek(-4-4-8-8-8, io.SeekEnd) // Skip magic, checksum, and placeholder for keys
	
	// Actually, let's just read the whole file for index parsing
	// This is simpler for the initial implementation
	file.Seek(0, io.SeekStart)
	data, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		return nil, err
	}
	
	sst := &SSTable{
		file: file,
		path: path,
	}
	
	// Parse footer from end
	if err := sst.parseFooterAndIndex(data, fileSize); err != nil {
		file.Close()
		return nil, err
	}
	
	return sst, nil
}

func (s *SSTable) parseFooterAndIndex(data []byte, fileSize int64) error {
	// Footer is at end: indexOffset(8) + indexSize(8) + entryCount(8) + minKeyLen(4) + minKey + maxKeyLen(4) + maxKey + checksum(4) + magic(4)
	if len(data) < 40 {
		return ErrCorruptedSSTable
	}
	
	// Read from end backwards
	offset := len(data)
	
	// Magic (4 bytes)
	offset -= 4
	magic := binary.LittleEndian.Uint32(data[offset:])
	if magic != sstableMagic {
		return ErrCorruptedSSTable
	}
	
	// Checksum (4 bytes) - we'll skip validation for now
	offset -= 4
	
	// To read footer, we need to know where minKey/maxKey start
	// We'll read forward from indexOffset location
	// First, find footerStart by scanning backwards past variable-length keys
	
	// Actually, let's compute footer from the front
	// Read indexOffset from a known position
	// The footer without variable keys would be 8+8+8+4+4+4+4 = 40 bytes minimum
	// But keys are variable, so we need a different approach
	
	// New approach: read index first, then keys are at fixed positions
	// Footer fixed part: indexOffset(8) + indexSize(8) + entryCount(8) = 24 bytes
	// Then: minKeyLen(4) + minKey(var) + maxKeyLen(4) + maxKey(var) + checksum(4) + magic(4)
	
	// Let's read the fixed footer first
	// We need to find where the footer starts
	// Since we know magic is valid at end, work backwards for checksum
	
	// SIMPLER: Let's scan backward to find index
	// Actually, let's just iterate through the file once to build index from data blocks
	
	// Parse data blocks from start until we hit index section
	s.index = make([]indexEntry, 0)
	blockOffset := int64(0)
	
	for blockOffset < int64(len(data))-40 { // Leave room for footer
		if int(blockOffset)+8 > len(data) {
			break
		}
		
		// Try to read block header
		checksum := binary.LittleEndian.Uint32(data[blockOffset:])
		blockLen := binary.LittleEndian.Uint32(data[blockOffset+4:])
		
		// Sanity check - block length should be reasonable
		if blockLen == 0 || blockLen > uint32(blockSize*2) || int(blockOffset)+8+int(blockLen) > len(data) {
			break // Reached index section
		}
		
		// Verify this is a valid block by checking CRC
		blockData := data[blockOffset+8 : blockOffset+8+int64(blockLen)]
		if crc32.ChecksumIEEE(blockData) != checksum {
			break // Not a valid block, must be index section
		}
		
		// Parse first entry to get firstKey
		if len(blockData) >= 16 {
			keyLen := binary.LittleEndian.Uint32(blockData[0:4])
			if int(keyLen) <= len(blockData)-16 {
				firstKey := make([]byte, keyLen)
				copy(firstKey, blockData[16:16+keyLen])
				
				s.index = append(s.index, indexEntry{
					firstKey:    firstKey,
					blockOffset: blockOffset,
					blockSize:   int64(8 + blockLen),
				})
				
				// Track min/max keys
				if len(s.minKey) == 0 {
					s.minKey = append([]byte{}, firstKey...)
				}
			}
		}
		
		blockOffset += int64(8 + blockLen)
	}
	
	// Set max key from last block's last entry
	if len(s.index) > 0 {
		lastBlock, err := s.readBlockFromData(data, len(s.index)-1)
		if err == nil && len(lastBlock) > 0 {
			s.maxKey = append([]byte{}, lastBlock[len(lastBlock)-1].Key...)
		}
	}
	
	return nil
}

func (s *SSTable) readBlockFromData(data []byte, idx int) ([]*Entry, error) {
	if idx >= len(s.index) {
		return nil, nil
	}
	
	blockEntry := s.index[idx]
	blockOffset := int(blockEntry.blockOffset)
	
	if blockOffset+8 > len(data) {
		return nil, ErrCorruptedSSTable
	}
	
	checksum := binary.LittleEndian.Uint32(data[blockOffset:])
	length := binary.LittleEndian.Uint32(data[blockOffset+4:])
	
	if blockOffset+8+int(length) > len(data) {
		return nil, ErrCorruptedSSTable
	}
	
	blockData := data[blockOffset+8 : blockOffset+8+int(length)]
	
	if crc32.ChecksumIEEE(blockData) != checksum {
		return nil, ErrCorruptedSSTable
	}
	
	return parseBlockEntries(blockData)
}

// Get looks up a key in the SSTable.
func (s *SSTable) Get(key []byte) ([]byte, bool) {
	// Binary search in index to find the right block
	blockIdx := sort.Search(len(s.index), func(i int) bool {
		return bytes.Compare(s.index[i].firstKey, key) > 0
	})
	
	if blockIdx > 0 {
		blockIdx--
	}
	
	if blockIdx >= len(s.index) {
		return nil, false
	}
	
	// Read and search the block
	block, err := s.readBlock(blockIdx)
	if err != nil {
		return nil, false
	}
	
	// Linear search within block
	for _, entry := range block {
		cmp := bytes.Compare(entry.Key, key)
		if cmp == 0 {
			if entry.Deleted {
				return nil, false
			}
			return entry.Value, true
		}
		if cmp > 0 {
			break
		}
	}
	
	return nil, false
}

func (s *SSTable) readBlock(idx int) ([]*Entry, error) {
	if idx >= len(s.index) {
		return nil, nil
	}
	
	blockEntry := s.index[idx]
	
	// Seek to block
	s.file.Seek(blockEntry.blockOffset, io.SeekStart)
	
	// Read CRC and length
	var checksum, length uint32
	binary.Read(s.file, binary.LittleEndian, &checksum)
	binary.Read(s.file, binary.LittleEndian, &length)
	
	// Read block data
	data := make([]byte, length)
	io.ReadFull(s.file, data)
	
	// Verify CRC
	if crc32.ChecksumIEEE(data) != checksum {
		return nil, ErrCorruptedSSTable
	}
	
	// Parse entries
	return parseBlockEntries(data)
}

func parseBlockEntries(data []byte) ([]*Entry, error) {
	entries := make([]*Entry, 0)
	offset := 0
	
	for offset < len(data) {
		if offset+16 > len(data) {
			break
		}
		
		// Key length
		keyLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		
		// Value length
		valueLen := int32(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		
		// Timestamp
		timestamp := binary.LittleEndian.Uint64(data[offset:])
		offset += 8
		
		// Key
		if offset+int(keyLen) > len(data) {
			break
		}
		key := make([]byte, keyLen)
		copy(key, data[offset:offset+int(keyLen)])
		offset += int(keyLen)
		
		// Value
		var value []byte
		deleted := valueLen < 0
		if !deleted {
			if offset+int(valueLen) > len(data) {
				break
			}
			value = make([]byte, valueLen)
			copy(value, data[offset:offset+int(valueLen)])
			offset += int(valueLen)
		}
		
		entries = append(entries, &Entry{
			Key:       key,
			Value:     value,
			Timestamp: timestamp,
			Deleted:   deleted,
		})
	}
	
	return entries, nil
}

// Contains checks if a key might be in this SSTable (using key range).
func (s *SSTable) Contains(key []byte) bool {
	if bytes.Compare(key, s.minKey) < 0 {
		return false
	}
	if bytes.Compare(key, s.maxKey) > 0 {
		return false
	}
	return true
}

// Close closes the SSTable file.
func (s *SSTable) Close() error {
	return s.file.Close()
}

// Path returns the file path.
func (s *SSTable) Path() string {
	return s.path
}

// EntryCount returns the number of entries in this SSTable.
func (s *SSTable) EntryCount() uint64 {
	return s.entryCount
}
