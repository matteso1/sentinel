package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetrics_RecordProduce(t *testing.T) {
	m := NewMetrics()

	m.RecordProduce(10, 1000, 5*time.Millisecond)
	m.RecordProduce(5, 500, 3*time.Millisecond)

	snap := m.Snapshot()

	if snap.MessagesProduced != 15 {
		t.Errorf("expected 15 messages produced, got %d", snap.MessagesProduced)
	}
	if snap.BytesProduced != 1500 {
		t.Errorf("expected 1500 bytes produced, got %d", snap.BytesProduced)
	}
}

func TestMetrics_RecordConsume(t *testing.T) {
	m := NewMetrics()

	m.RecordConsume(20, 2000, 10*time.Millisecond)

	snap := m.Snapshot()

	if snap.MessagesConsumed != 20 {
		t.Errorf("expected 20 messages consumed, got %d", snap.MessagesConsumed)
	}
	if snap.BytesConsumed != 2000 {
		t.Errorf("expected 2000 bytes consumed, got %d", snap.BytesConsumed)
	}
}

func TestMetrics_Connections(t *testing.T) {
	m := NewMetrics()

	m.ConnectionOpened()
	m.ConnectionOpened()
	m.ConnectionOpened()
	m.ConnectionClosed()

	snap := m.Snapshot()

	if snap.ActiveConnections != 2 {
		t.Errorf("expected 2 active connections, got %d", snap.ActiveConnections)
	}
}

func TestMetrics_Errors(t *testing.T) {
	m := NewMetrics()

	m.RecordError()
	m.RecordError()

	snap := m.Snapshot()

	if snap.ErrorsTotal != 2 {
		t.Errorf("expected 2 errors, got %d", snap.ErrorsTotal)
	}
}

func TestMetrics_Handler(t *testing.T) {
	m := NewMetrics()

	m.RecordProduce(100, 10000, 5*time.Millisecond)
	m.RecordConsume(50, 5000, 3*time.Millisecond)
	m.RecordError()
	m.ConnectionOpened()

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()

	m.Handler()(rec, req)

	body := rec.Body.String()

	// Check that key metrics are present
	checks := []string{
		"sentinel_uptime_seconds",
		"sentinel_messages_produced_total 100",
		"sentinel_messages_consumed_total 50",
		"sentinel_bytes_produced_total 10000",
		"sentinel_bytes_consumed_total 5000",
		"sentinel_errors_total 1",
		"sentinel_active_connections 1",
	}

	for _, check := range checks {
		if !strings.Contains(body, check) {
			t.Errorf("expected %q in metrics output", check)
		}
	}
}

func TestMetrics_PartitionLag(t *testing.T) {
	m := NewMetrics()

	m.SetPartitionLag("events", 0, 100)
	m.SetPartitionLag("events", 1, 50)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()

	m.Handler()(rec, req)

	body := rec.Body.String()

	if !strings.Contains(body, "sentinel_partition_lag") {
		t.Error("expected partition lag metrics")
	}
}
