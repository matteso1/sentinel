package broker

import (
	"fmt"
	"os"
	"testing"
)

func TestBroker_ProduceConsume(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-broker-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := DefaultBrokerConfig()
	config.DataDir = dir

	broker, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()

	// Produce records
	records := []*Record{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}

	baseOffset, err := broker.Produce("test-topic", 0, records)
	if err != nil {
		t.Fatal(err)
	}
	if baseOffset != 0 {
		t.Errorf("expected base offset 0, got %d", baseOffset)
	}

	// Consume records
	consumed, err := broker.Consume("test-topic", 0, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(consumed) != 3 {
		t.Errorf("expected 3 records, got %d", len(consumed))
	}
}

func TestProducer_Partitioner(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-producer-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := DefaultBrokerConfig()
	config.DataDir = dir
	config.DefaultPartitions = 3

	broker, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()

	// Create producer with hash partitioner
	producer := NewProducer(broker, ProducerConfig{
		Partitioner: &HashPartitioner{},
	})

	// Send records with same key - should go to same partition
	key := []byte("user-123")
	for i := 0; i < 10; i++ {
		_, err := producer.Send("events", -1, key, []byte(fmt.Sprintf("event-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Verify all records went to same partition
	topic, _ := broker.GetTopic("events", false)
	nonEmptyPartitions := 0
	for _, p := range topic.Partitions() {
		if p.HighWatermark() > 0 {
			nonEmptyPartitions++
			if p.HighWatermark() != 10 {
				t.Errorf("expected all 10 records in one partition, got %d", p.HighWatermark())
			}
		}
	}
	if nonEmptyPartitions != 1 {
		t.Errorf("expected 1 non-empty partition, got %d", nonEmptyPartitions)
	}
}

func TestConsumerClient_Poll(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-consumer-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := DefaultBrokerConfig()
	config.DataDir = dir

	broker, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()

	// Produce some records
	for i := 0; i < 5; i++ {
		broker.Produce("logs", 0, []*Record{{Value: []byte(fmt.Sprintf("log-%d", i))}})
	}

	// Create consumer
	consumer, err := NewConsumerClient(broker, ConsumerClientConfig{
		Topic:     "logs",
		Partition: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Poll first batch
	records, err := consumer.Poll(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Errorf("expected 3 records, got %d", len(records))
	}

	// Poll second batch
	records, err = consumer.Poll(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Errorf("expected 2 records, got %d", len(records))
	}

	// Poll again - should be empty
	records, err = consumer.Poll(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records, got %d", len(records))
	}
}

func TestConsumerGroup_Rebalance(t *testing.T) {
	cg := NewConsumerGroup("my-group", "my-topic")

	// Add consumers
	cg.Join("consumer-1")
	cg.Join("consumer-2")

	// Rebalance with 6 partitions
	cg.Rebalance(6)

	// Verify assignment (round-robin)
	members := cg.Members()
	for _, m := range members {
		if len(m.partitions) != 3 {
			t.Errorf("expected 3 partitions per consumer, got %d", len(m.partitions))
		}
	}
}
