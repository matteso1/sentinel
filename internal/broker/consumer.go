package broker

import (
	"fmt"
	"sync"
)

// Consumer reads records from partitions.
type ConsumerClient struct {
	broker     *Broker
	topic      string
	partitions []int32
	offsets    map[int32]int64 // partition -> current offset
	group      *ConsumerGroup
	mu         sync.Mutex
}

// ConsumerClientConfig configures the consumer.
type ConsumerClientConfig struct {
	Topic     string
	GroupID   string
	Partition int32 // -1 = all partitions
}

// NewConsumerClient creates a new consumer.
func NewConsumerClient(broker *Broker, config ConsumerClientConfig) (*ConsumerClient, error) {
	topic, err := broker.GetTopic(config.Topic, false)
	if err != nil {
		return nil, err
	}

	c := &ConsumerClient{
		broker:  broker,
		topic:   config.Topic,
		offsets: make(map[int32]int64),
	}

	if config.Partition >= 0 {
		// Specific partition
		c.partitions = []int32{config.Partition}
	} else {
		// All partitions
		c.partitions = make([]int32, topic.NumPartitions())
		for i := range c.partitions {
			c.partitions[i] = int32(i)
		}
	}

	// Initialize offsets to 0
	for _, p := range c.partitions {
		c.offsets[p] = 0
	}

	return c, nil
}

// Poll fetches the next batch of records.
func (c *ConsumerClient) Poll(maxRecords int) ([]*Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var allRecords []*Record

	for _, partition := range c.partitions {
		offset := c.offsets[partition]
		records, err := c.broker.Consume(c.topic, partition, offset, maxRecords)
		if err != nil {
			continue
		}

		if len(records) > 0 {
			allRecords = append(allRecords, records...)
			c.offsets[partition] = records[len(records)-1].Offset + 1
		}
	}

	return allRecords, nil
}

// Seek sets the offset for a partition.
func (c *ConsumerClient) Seek(partition int32, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	found := false
	for _, p := range c.partitions {
		if p == partition {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("partition %d not assigned to this consumer", partition)
	}

	c.offsets[partition] = offset
	return nil
}

// SeekToBeginning resets all partitions to the earliest offset.
func (c *ConsumerClient) SeekToBeginning() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for p := range c.offsets {
		c.offsets[p] = 0
	}
}

// SeekToEnd sets all partitions to the latest offset.
func (c *ConsumerClient) SeekToEnd() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	topic, err := c.broker.GetTopic(c.topic, false)
	if err != nil {
		return err
	}

	for _, partitionID := range c.partitions {
		p, err := topic.GetPartition(partitionID)
		if err != nil {
			continue
		}
		c.offsets[partitionID] = p.HighWatermark()
	}
	return nil
}

// Position returns the current offset for a partition.
func (c *ConsumerClient) Position(partition int32) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offsets[partition]
}

// Commit commits the current offsets (for consumer groups).
func (c *ConsumerClient) Commit() error {
	if c.group == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for partition, offset := range c.offsets {
		c.group.CommitOffset(partition, offset)
	}
	return nil
}
