package broker

import (
	"sync"
	"sync/atomic"
)

// Partition is an ordered, immutable sequence of records.
// Each partition is an independent, append-only log.
type Partition struct {
	id            int32
	highWatermark int64      // Next offset to be written
	mu            sync.Mutex // Protects writes
}

// NewPartition creates a new partition.
func NewPartition(id int32) *Partition {
	return &Partition{
		id:            id,
		highWatermark: 0,
	}
}

// ID returns the partition ID.
func (p *Partition) ID() int32 {
	return p.id
}

// HighWatermark returns the offset of the next record to be written.
func (p *Partition) HighWatermark() int64 {
	return atomic.LoadInt64(&p.highWatermark)
}

// Append adds records to the partition and returns the base offset.
func (p *Partition) Append(records []*Record) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	baseOffset := p.highWatermark
	for i, record := range records {
		record.Offset = baseOffset + int64(i)
	}

	atomic.AddInt64(&p.highWatermark, int64(len(records)))
	return baseOffset
}

// Stats returns partition statistics.
type PartitionStats struct {
	ID            int32
	HighWatermark int64
	Size          int64
}

// Stats returns current partition statistics.
func (p *Partition) Stats() PartitionStats {
	return PartitionStats{
		ID:            p.id,
		HighWatermark: p.HighWatermark(),
	}
}
