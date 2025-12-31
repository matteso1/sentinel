package broker

import (
	"fmt"
	"sync"
)

// Topic represents a named stream of messages divided into partitions.
type Topic struct {
	name       string
	partitions []*Partition
	mu         sync.RWMutex
}

// NewTopic creates a new topic with the specified number of partitions.
func NewTopic(name string, numPartitions int) *Topic {
	if numPartitions < 1 {
		numPartitions = 1
	}

	partitions := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partitions[i] = NewPartition(int32(i))
	}

	return &Topic{
		name:       name,
		partitions: partitions,
	}
}

// Name returns the topic name.
func (t *Topic) Name() string {
	return t.name
}

// NumPartitions returns the number of partitions.
func (t *Topic) NumPartitions() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.partitions)
}

// GetPartition returns a partition by ID.
func (t *Topic) GetPartition(id int32) (*Partition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if int(id) >= len(t.partitions) {
		return nil, fmt.Errorf("partition %d does not exist (topic has %d partitions)", id, len(t.partitions))
	}
	return t.partitions[id], nil
}

// Partitions returns all partitions.
func (t *Topic) Partitions() []*Partition {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.partitions
}
