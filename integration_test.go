package sentinel_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/matteso1/sentinel/internal/broker"
	"github.com/matteso1/sentinel/internal/storage"
)

// Integration tests verify end-to-end functionality across components.

func TestE2E_ProduceConsume(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-e2e-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := broker.DefaultBrokerConfig()
	config.DataDir = dir

	b, err := broker.NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// Produce messages
	records := []*broker.Record{
		{Key: []byte("user-1"), Value: []byte("login")},
		{Key: []byte("user-2"), Value: []byte("click")},
		{Key: []byte("user-1"), Value: []byte("logout")},
	}

	baseOffset, err := b.Produce("events", 0, records)
	if err != nil {
		t.Fatal(err)
	}
	if baseOffset != 0 {
		t.Errorf("expected base offset 0, got %d", baseOffset)
	}

	// Consume messages back
	consumed, err := b.Consume("events", 0, 0, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(consumed) != 3 {
		t.Errorf("expected 3 records, got %d", len(consumed))
	}

	// Verify values match (note: broker only stores values, not keys)
	for i, record := range consumed {
		if string(record.Value) != string(records[i].Value) {
			t.Errorf("record %d: expected value %q, got %q", i, records[i].Value, record.Value)
		}
	}
}

func TestE2E_MultipleTopics(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-e2e-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := broker.DefaultBrokerConfig()
	config.DataDir = dir

	b, err := broker.NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// Produce to multiple topics
	topics := []string{"logs", "events", "metrics"}
	for i, topic := range topics {
		records := []*broker.Record{
			{Value: []byte(fmt.Sprintf("message-%d-a", i))},
			{Value: []byte(fmt.Sprintf("message-%d-b", i))},
		}
		_, err := b.Produce(topic, 0, records)
		if err != nil {
			t.Fatalf("failed to produce to %s: %v", topic, err)
		}
	}

	// Verify each topic has its own data
	for i, topic := range topics {
		consumed, err := b.Consume(topic, 0, 0, 10)
		if err != nil {
			t.Fatalf("failed to consume from %s: %v", topic, err)
		}
		if len(consumed) != 2 {
			t.Errorf("topic %s: expected 2 records, got %d", topic, len(consumed))
		}
		expected := fmt.Sprintf("message-%d-a", i)
		if string(consumed[0].Value) != expected {
			t.Errorf("topic %s: expected %q, got %q", topic, expected, consumed[0].Value)
		}
	}
}

func TestE2E_LargeWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large workload test in short mode")
	}

	dir, err := os.MkdirTemp("", "sentinel-e2e-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := broker.DefaultBrokerConfig()
	config.DataDir = dir

	b, err := broker.NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// Produce 10K messages in batches
	numMessages := 10000
	batchSize := 100

	for i := 0; i < numMessages; i += batchSize {
		records := make([]*broker.Record, batchSize)
		for j := 0; j < batchSize; j++ {
			records[j] = &broker.Record{
				Key:   []byte(fmt.Sprintf("key-%06d", i+j)),
				Value: []byte(fmt.Sprintf("value-%06d", i+j)),
			}
		}
		_, err := b.Produce("stress-test", 0, records)
		if err != nil {
			t.Fatalf("failed at batch %d: %v", i/batchSize, err)
		}
	}

	// Verify all messages are there
	consumed, err := b.Consume("stress-test", 0, 0, numMessages+100)
	if err != nil {
		t.Fatal(err)
	}

	if len(consumed) != numMessages {
		t.Errorf("expected %d records, got %d", numMessages, len(consumed))
	}

	t.Logf("Successfully produced and consumed %d messages", numMessages)
}

func TestE2E_DataPersistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-e2e-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// First: Create LSM, write data, close
	{
		config := storage.DefaultLSMConfig()
		config.MemTableSize = 1024 * 1024 // 1MB

		lsm, err := storage.Open(dir, config)
		if err != nil {
			t.Fatal(err)
		}

		// Write some data
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("persist-key-%03d", i))
			value := []byte(fmt.Sprintf("persist-value-%03d", i))
			if err := lsm.Put(key, value); err != nil {
				t.Fatal(err)
			}
		}

		// Force sync and close
		if err := lsm.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Second: Reopen and verify data is still there
	{
		config := storage.DefaultLSMConfig()
		lsm, err := storage.Open(dir, config)
		if err != nil {
			t.Fatal(err)
		}
		defer lsm.Close()

		// Verify data persisted
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("persist-key-%03d", i))
			expected := fmt.Sprintf("persist-value-%03d", i)

			value, err := lsm.Get(key)
			if err != nil {
				t.Errorf("failed to get key %s: %v", key, err)
				continue
			}
			if string(value) != expected {
				t.Errorf("key %s: expected %q, got %q", key, expected, value)
			}
		}

		t.Log("Data persistence verified across close/reopen")
	}
}

func TestE2E_OffsetSemantics(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-e2e-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := broker.DefaultBrokerConfig()
	config.DataDir = dir

	b, err := broker.NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// Produce first batch
	batch1 := []*broker.Record{
		{Value: []byte("msg-0")},
		{Value: []byte("msg-1")},
		{Value: []byte("msg-2")},
	}
	offset1, err := b.Produce("offsets", 0, batch1)
	if err != nil {
		t.Fatal(err)
	}
	if offset1 != 0 {
		t.Errorf("expected first batch offset 0, got %d", offset1)
	}

	// Produce second batch
	batch2 := []*broker.Record{
		{Value: []byte("msg-3")},
		{Value: []byte("msg-4")},
	}
	offset2, err := b.Produce("offsets", 0, batch2)
	if err != nil {
		t.Fatal(err)
	}
	if offset2 != 3 {
		t.Errorf("expected second batch offset 3, got %d", offset2)
	}

	// Consume from middle
	consumed, err := b.Consume("offsets", 0, 2, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Should get msg-2, msg-3, msg-4
	if len(consumed) != 3 {
		t.Errorf("expected 3 records from offset 2, got %d", len(consumed))
	}

	if len(consumed) > 0 && string(consumed[0].Value) != "msg-2" {
		t.Errorf("expected first consumed message to be 'msg-2', got %q", consumed[0].Value)
	}
}
