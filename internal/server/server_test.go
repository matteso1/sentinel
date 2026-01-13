package server

import (
	"os"
	"testing"
)

func TestServer_Creation(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-server-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := DefaultServerConfig()
	config.DataDir = dir
	config.Port = 19092 // Use different port to avoid conflicts

	srv, err := NewServer(config)
	if err != nil {
		t.Fatal(err)
	}

	if srv == nil {
		t.Error("expected server to be created")
	}

	srv.Stop()
}

func TestServer_TopicCreation(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-server-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := DefaultServerConfig()
	config.DataDir = dir
	config.Port = 19093

	srv, err := NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	// Verify topic map is initialized
	if srv.topics == nil {
		t.Error("expected topics map to be initialized")
	}
}

func TestServer_MakeKey(t *testing.T) {
	dir, err := os.MkdirTemp("", "sentinel-server-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := DefaultServerConfig()
	config.DataDir = dir

	srv, err := NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	// Test key format
	key := srv.makeKey("events", 0, 42)
	expected := "events:0:00000000000000000042"
	if string(key) != expected {
		t.Errorf("expected key %q, got %q", expected, string(key))
	}

	// Test different partition
	key2 := srv.makeKey("logs", 3, 100)
	expected2 := "logs:3:00000000000000000100"
	if string(key2) != expected2 {
		t.Errorf("expected key %q, got %q", expected2, string(key2))
	}
}

func TestServer_DefaultConfig(t *testing.T) {
	config := DefaultServerConfig()

	if config.Port != 9092 {
		t.Errorf("expected default port 9092, got %d", config.Port)
	}

	if config.DataDir != "./data" {
		t.Errorf("expected default data dir ./data, got %s", config.DataDir)
	}
}

func TestPartition_Locking(t *testing.T) {
	p := &Partition{
		id:         0,
		nextOffset: 0,
	}

	// Test that we can lock/unlock without deadlock
	p.mu.Lock()
	p.nextOffset = 100
	p.mu.Unlock()

	if p.nextOffset != 100 {
		t.Errorf("expected offset 100, got %d", p.nextOffset)
	}
}

func TestTopic_Creation(t *testing.T) {
	topic := &Topic{
		name:       "test-topic",
		partitions: []*Partition{{id: 0}, {id: 1}, {id: 2}},
	}

	if topic.name != "test-topic" {
		t.Errorf("expected topic name 'test-topic', got %q", topic.name)
	}

	if len(topic.partitions) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(topic.partitions))
	}
}
