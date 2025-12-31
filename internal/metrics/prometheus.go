package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects and exposes Prometheus-style metrics.
type Metrics struct {
	// Counters
	messagesProduced atomic.Uint64
	messagesConsumed atomic.Uint64
	bytesProduced    atomic.Uint64
	bytesConsumed    atomic.Uint64
	errorsTotal      atomic.Uint64

	// Gauges
	activeConnections atomic.Int64
	partitionLag      sync.Map // topic:partition -> lag

	// Histograms (simplified as averages)
	produceLatencySum atomic.Uint64
	produceLatencyN   atomic.Uint64
	consumeLatencySum atomic.Uint64
	consumeLatencyN   atomic.Uint64

	startTime time.Time
}

// NewMetrics creates a new metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

// RecordProduce records a produce operation.
func (m *Metrics) RecordProduce(count int, bytes int, latency time.Duration) {
	m.messagesProduced.Add(uint64(count))
	m.bytesProduced.Add(uint64(bytes))
	m.produceLatencySum.Add(uint64(latency.Microseconds()))
	m.produceLatencyN.Add(1)
}

// RecordConsume records a consume operation.
func (m *Metrics) RecordConsume(count int, bytes int, latency time.Duration) {
	m.messagesConsumed.Add(uint64(count))
	m.bytesConsumed.Add(uint64(bytes))
	m.consumeLatencySum.Add(uint64(latency.Microseconds()))
	m.consumeLatencyN.Add(1)
}

// RecordError records an error.
func (m *Metrics) RecordError() {
	m.errorsTotal.Add(1)
}

// ConnectionOpened increments active connections.
func (m *Metrics) ConnectionOpened() {
	m.activeConnections.Add(1)
}

// ConnectionClosed decrements active connections.
func (m *Metrics) ConnectionClosed() {
	m.activeConnections.Add(-1)
}

// SetPartitionLag sets the lag for a partition.
func (m *Metrics) SetPartitionLag(topic string, partition int, lag int64) {
	key := fmt.Sprintf("%s:%d", topic, partition)
	m.partitionLag.Store(key, lag)
}

// Handler returns an HTTP handler for the /metrics endpoint.
func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		// Uptime
		uptime := time.Since(m.startTime).Seconds()
		fmt.Fprintf(w, "# HELP sentinel_uptime_seconds Time since server started\n")
		fmt.Fprintf(w, "# TYPE sentinel_uptime_seconds gauge\n")
		fmt.Fprintf(w, "sentinel_uptime_seconds %.2f\n\n", uptime)

		// Messages produced
		fmt.Fprintf(w, "# HELP sentinel_messages_produced_total Total messages produced\n")
		fmt.Fprintf(w, "# TYPE sentinel_messages_produced_total counter\n")
		fmt.Fprintf(w, "sentinel_messages_produced_total %d\n\n", m.messagesProduced.Load())

		// Messages consumed
		fmt.Fprintf(w, "# HELP sentinel_messages_consumed_total Total messages consumed\n")
		fmt.Fprintf(w, "# TYPE sentinel_messages_consumed_total counter\n")
		fmt.Fprintf(w, "sentinel_messages_consumed_total %d\n\n", m.messagesConsumed.Load())

		// Bytes produced
		fmt.Fprintf(w, "# HELP sentinel_bytes_produced_total Total bytes produced\n")
		fmt.Fprintf(w, "# TYPE sentinel_bytes_produced_total counter\n")
		fmt.Fprintf(w, "sentinel_bytes_produced_total %d\n\n", m.bytesProduced.Load())

		// Bytes consumed
		fmt.Fprintf(w, "# HELP sentinel_bytes_consumed_total Total bytes consumed\n")
		fmt.Fprintf(w, "# TYPE sentinel_bytes_consumed_total counter\n")
		fmt.Fprintf(w, "sentinel_bytes_consumed_total %d\n\n", m.bytesConsumed.Load())

		// Errors
		fmt.Fprintf(w, "# HELP sentinel_errors_total Total errors\n")
		fmt.Fprintf(w, "# TYPE sentinel_errors_total counter\n")
		fmt.Fprintf(w, "sentinel_errors_total %d\n\n", m.errorsTotal.Load())

		// Active connections
		fmt.Fprintf(w, "# HELP sentinel_active_connections Current active connections\n")
		fmt.Fprintf(w, "# TYPE sentinel_active_connections gauge\n")
		fmt.Fprintf(w, "sentinel_active_connections %d\n\n", m.activeConnections.Load())

		// Average produce latency
		produceN := m.produceLatencyN.Load()
		if produceN > 0 {
			avgProduceLatency := float64(m.produceLatencySum.Load()) / float64(produceN) / 1000.0 // ms
			fmt.Fprintf(w, "# HELP sentinel_produce_latency_ms Average produce latency\n")
			fmt.Fprintf(w, "# TYPE sentinel_produce_latency_ms gauge\n")
			fmt.Fprintf(w, "sentinel_produce_latency_ms %.2f\n\n", avgProduceLatency)
		}

		// Average consume latency
		consumeN := m.consumeLatencyN.Load()
		if consumeN > 0 {
			avgConsumeLatency := float64(m.consumeLatencySum.Load()) / float64(consumeN) / 1000.0
			fmt.Fprintf(w, "# HELP sentinel_consume_latency_ms Average consume latency\n")
			fmt.Fprintf(w, "# TYPE sentinel_consume_latency_ms gauge\n")
			fmt.Fprintf(w, "sentinel_consume_latency_ms %.2f\n\n", avgConsumeLatency)
		}

		// Partition lag
		fmt.Fprintf(w, "# HELP sentinel_partition_lag Lag per partition\n")
		fmt.Fprintf(w, "# TYPE sentinel_partition_lag gauge\n")
		m.partitionLag.Range(func(key, value interface{}) bool {
			fmt.Fprintf(w, "sentinel_partition_lag{partition=\"%s\"} %d\n", key, value.(int64))
			return true
		})
	}
}

// Snapshot returns current metric values.
type Snapshot struct {
	MessagesProduced  uint64
	MessagesConsumed  uint64
	BytesProduced     uint64
	BytesConsumed     uint64
	ErrorsTotal       uint64
	ActiveConnections int64
	UptimeSeconds     float64
}

// Snapshot returns a snapshot of current metrics.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		MessagesProduced:  m.messagesProduced.Load(),
		MessagesConsumed:  m.messagesConsumed.Load(),
		BytesProduced:     m.bytesProduced.Load(),
		BytesConsumed:     m.bytesConsumed.Load(),
		ErrorsTotal:       m.errorsTotal.Load(),
		ActiveConnections: m.activeConnections.Load(),
		UptimeSeconds:     time.Since(m.startTime).Seconds(),
	}
}
