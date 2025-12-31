package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/matteso1/sentinel/internal/storage"
)

// Broker is the main message broker managing topics, partitions, and consumers.
type Broker struct {
	id     int32
	topics map[string]*Topic
	store  *storage.LSM
	mu     sync.RWMutex
	config BrokerConfig
}

// BrokerConfig configures the broker.
type BrokerConfig struct {
	ID                       int32
	DefaultPartitions        int
	DefaultReplicationFactor int
	DataDir                  string
	LSMConfig                storage.LSMConfig
}

// DefaultBrokerConfig returns sensible defaults.
func DefaultBrokerConfig() BrokerConfig {
	return BrokerConfig{
		ID:                       0,
		DefaultPartitions:        3,
		DefaultReplicationFactor: 1,
		DataDir:                  "./data",
		LSMConfig:                storage.DefaultLSMConfig(),
	}
}

// NewBroker creates a new message broker.
func NewBroker(config BrokerConfig) (*Broker, error) {
	store, err := storage.Open(config.DataDir, config.LSMConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage: %w", err)
	}

	return &Broker{
		id:     config.ID,
		topics: make(map[string]*Topic),
		store:  store,
		config: config,
	}, nil
}

// CreateTopic creates a new topic with the specified number of partitions.
func (b *Broker) CreateTopic(name string, partitions int) (*Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return nil, fmt.Errorf("topic %s already exists", name)
	}

	topic := NewTopic(name, partitions)
	b.topics[name] = topic
	return topic, nil
}

// GetTopic returns a topic by name, optionally auto-creating it.
func (b *Broker) GetTopic(name string, autoCreate bool) (*Topic, error) {
	b.mu.RLock()
	topic, exists := b.topics[name]
	b.mu.RUnlock()

	if exists {
		return topic, nil
	}

	if !autoCreate {
		return nil, fmt.Errorf("topic %s not found", name)
	}

	return b.CreateTopic(name, b.config.DefaultPartitions)
}

// DeleteTopic removes a topic.
func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; !exists {
		return fmt.Errorf("topic %s not found", name)
	}

	delete(b.topics, name)
	return nil
}

// ListTopics returns all topic names.
func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	return names
}

// Produce appends records to a partition.
func (b *Broker) Produce(topicName string, partition int32, records []*Record) (int64, error) {
	topic, err := b.GetTopic(topicName, true)
	if err != nil {
		return 0, err
	}

	p, err := topic.GetPartition(partition)
	if err != nil {
		return 0, err
	}

	baseOffset := p.Append(records)

	// Persist to storage
	for i, record := range records {
		key := b.makeKey(topicName, partition, baseOffset+int64(i))
		if err := b.store.Put(key, record.Value); err != nil {
			return 0, fmt.Errorf("failed to persist record: %w", err)
		}
	}

	return baseOffset, nil
}

// Consume reads records from a partition starting at offset.
func (b *Broker) Consume(topicName string, partition int32, offset int64, maxRecords int) ([]*Record, error) {
	topic, err := b.GetTopic(topicName, false)
	if err != nil {
		return nil, err
	}

	p, err := topic.GetPartition(partition)
	if err != nil {
		return nil, err
	}

	highWatermark := p.HighWatermark()
	if offset >= highWatermark {
		return nil, nil // No new records
	}

	records := make([]*Record, 0, maxRecords)
	for i := 0; i < maxRecords && offset+int64(i) < highWatermark; i++ {
		key := b.makeKey(topicName, partition, offset+int64(i))
		value, err := b.store.Get(key)
		if err != nil {
			continue
		}

		records = append(records, &Record{
			Key:       nil,
			Value:     value,
			Offset:    offset + int64(i),
			Timestamp: time.Now().UnixNano(),
		})
	}

	return records, nil
}

// makeKey creates a storage key for topic/partition/offset.
func (b *Broker) makeKey(topic string, partition int32, offset int64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%020d", topic, partition, offset))
}

// Close shuts down the broker.
func (b *Broker) Close() error {
	return b.store.Close()
}

// Record represents a message in the log.
type Record struct {
	Key       []byte
	Value     []byte
	Offset    int64
	Timestamp int64
}
