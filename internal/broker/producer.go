package broker

import (
	"hash/fnv"
)

// Partitioner determines which partition a record should be sent to.
type Partitioner interface {
	Partition(key []byte, numPartitions int) int32
}

// RoundRobinPartitioner distributes records evenly across partitions.
type RoundRobinPartitioner struct {
	counter int64
}

// Partition returns the next partition in round-robin order.
func (p *RoundRobinPartitioner) Partition(key []byte, numPartitions int) int32 {
	p.counter++
	return int32(p.counter % int64(numPartitions))
}

// HashPartitioner uses the key hash to determine the partition.
// Records with the same key always go to the same partition.
type HashPartitioner struct{}

// Partition returns the partition based on key hash.
func (p *HashPartitioner) Partition(key []byte, numPartitions int) int32 {
	if len(key) == 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(numPartitions))
}

// Producer sends records to topics.
type Producer struct {
	broker      *Broker
	partitioner Partitioner
}

// ProducerConfig configures the producer.
type ProducerConfig struct {
	Partitioner Partitioner
}

// DefaultProducerConfig returns sensible defaults.
func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		Partitioner: &RoundRobinPartitioner{},
	}
}

// NewProducer creates a new producer.
func NewProducer(broker *Broker, config ProducerConfig) *Producer {
	if config.Partitioner == nil {
		config.Partitioner = &RoundRobinPartitioner{}
	}
	return &Producer{
		broker:      broker,
		partitioner: config.Partitioner,
	}
}

// Send sends a single record to a topic.
// If partition is -1, the partitioner determines the partition.
func (p *Producer) Send(topic string, partition int32, key, value []byte) (int64, error) {
	if partition < 0 {
		t, err := p.broker.GetTopic(topic, true)
		if err != nil {
			return 0, err
		}
		partition = p.partitioner.Partition(key, t.NumPartitions())
	}

	record := &Record{
		Key:   key,
		Value: value,
	}
	return p.broker.Produce(topic, partition, []*Record{record})
}

// SendBatch sends multiple records to a topic partition.
func (p *Producer) SendBatch(topic string, partition int32, records []*Record) (int64, error) {
	return p.broker.Produce(topic, partition, records)
}
