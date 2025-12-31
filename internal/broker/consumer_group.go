package broker

import (
	"sync"
	"time"
)

// ConsumerGroup manages a group of consumers sharing partition ownership.
// Partitions are assigned to consumers in a round-robin fashion.
type ConsumerGroup struct {
	groupID    string
	topic      string
	members    map[string]*Consumer
	offsets    map[int32]int64 // partition -> committed offset
	generation int32
	mu         sync.RWMutex
}

// Consumer represents a member of a consumer group.
type Consumer struct {
	id            string
	groupID       string
	partitions    []int32 // Assigned partitions
	lastHeartbeat time.Time
}

// NewConsumerGroup creates a new consumer group.
func NewConsumerGroup(groupID, topic string) *ConsumerGroup {
	return &ConsumerGroup{
		groupID:    groupID,
		topic:      topic,
		members:    make(map[string]*Consumer),
		offsets:    make(map[int32]int64),
		generation: 0,
	}
}

// Join adds a consumer to the group and triggers rebalance.
func (cg *ConsumerGroup) Join(consumerID string) (*Consumer, error) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	consumer := &Consumer{
		id:            consumerID,
		groupID:       cg.groupID,
		lastHeartbeat: time.Now(),
	}
	cg.members[consumerID] = consumer
	cg.generation++

	return consumer, nil
}

// Leave removes a consumer from the group.
func (cg *ConsumerGroup) Leave(consumerID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	delete(cg.members, consumerID)
	cg.generation++
}

// Heartbeat updates the last heartbeat time for a consumer.
func (cg *ConsumerGroup) Heartbeat(consumerID string) bool {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	consumer, exists := cg.members[consumerID]
	if !exists {
		return false
	}
	consumer.lastHeartbeat = time.Now()
	return true
}

// CommitOffset stores the committed offset for a partition.
func (cg *ConsumerGroup) CommitOffset(partition int32, offset int64) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.offsets[partition] = offset
}

// GetOffset returns the committed offset for a partition.
func (cg *ConsumerGroup) GetOffset(partition int32) (int64, bool) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	offset, exists := cg.offsets[partition]
	return offset, exists
}

// Rebalance assigns partitions to consumers using round-robin.
func (cg *ConsumerGroup) Rebalance(numPartitions int) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	// Clear existing assignments
	for _, consumer := range cg.members {
		consumer.partitions = nil
	}

	if len(cg.members) == 0 {
		return
	}

	// Build ordered list of consumers
	consumers := make([]*Consumer, 0, len(cg.members))
	for _, c := range cg.members {
		consumers = append(consumers, c)
	}

	// Round-robin partition assignment
	for p := 0; p < numPartitions; p++ {
		consumer := consumers[p%len(consumers)]
		consumer.partitions = append(consumer.partitions, int32(p))
	}

	cg.generation++
}

// Members returns all group members.
func (cg *ConsumerGroup) Members() []*Consumer {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	members := make([]*Consumer, 0, len(cg.members))
	for _, c := range cg.members {
		members = append(members, c)
	}
	return members
}

// Generation returns the current group generation.
func (cg *ConsumerGroup) Generation() int32 {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return cg.generation
}
