package tsdb

import (
	"testing"
	"time"

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/util/compression"
	"github.com/stretchr/testify/require"
)

func TestOOOBufferAdd(t *testing.T) {
	// Simple sanity check
	h := &Head{}
	cfg := config.ExemplarOutOfOrderConfig{}
	s := NewOutOfOrderExemplarStorage(h, cfg)
	t.Cleanup(s.Stop)
	_ = s
}

func TestOOOBufferAddAndFlushBySize(t *testing.T) {
	h, _ := newTestHead(t, 1000, compression.None, false)
	defer func() { require.NoError(t, h.Close()) }()

	cfg := config.ExemplarOutOfOrderConfig{
		TimeWindow:              model.Duration(30 * time.Second),
		MaxBufferSizePerSeries:  3,
		MaxTotalBufferSizeBytes: 1 << 20,
	}
       s := NewOutOfOrderExemplarStorage(h, cfg)
       // Stop background workers to avoid race with flush queue consumption.
       close(s.stop)
       close(s.flushQueue)
       s.wg.Wait()
       s.flushQueue = make(chan uint64, 10000)

	id := uint64(1)
	for i := 0; i < 2; i++ {
		require.NoError(t, s.Add(id, exemplar.Exemplar{Ts: int64(i + 1)}))
	}
	if l := len(s.flushQueue); l != 0 {
		t.Fatalf("expected empty flush queue, got %d", l)
	}

	require.NoError(t, s.Add(id, exemplar.Exemplar{Ts: 3}))
	if l := len(s.flushQueue); l != 1 {
		t.Fatalf("expected one item in flush queue, got %d", l)
	}

	require.NoError(t, s.Add(id, exemplar.Exemplar{Ts: 4}))
	if l := len(s.flushQueue); l != 2 {
		t.Fatalf("expected two items in flush queue, got %d", l)
	}
	s.mtx.RLock()
	buf, ok := s.buffers[id]
	s.mtx.RUnlock()
	if !ok {
		t.Fatalf("buffer unexpectedly flushed")
	}
	if len(buf.exemplars) != 4 {
		t.Fatalf("exemplar should have been buffered")
	}
}

func TestCircuitBreakerTripsAndResets(t *testing.T) {
	h, _ := newTestHead(t, 1000, compression.None, false)
	defer func() { require.NoError(t, h.Close()) }()

	cfg := config.ExemplarOutOfOrderConfig{
		TimeWindow:              model.Duration(30 * time.Second),
		MaxBufferSizePerSeries:  10,
		MaxTotalBufferSizeBytes: 200,
	}
	s := NewOutOfOrderExemplarStorage(h, cfg)
	t.Cleanup(s.Stop)

	for i := uint64(1); i <= 3; i++ {
		require.NoError(t, s.Add(i, exemplar.Exemplar{Ts: int64(i)}))
	}

	require.NoError(t, s.Add(4, exemplar.Exemplar{Ts: 4}))
	if prom_testutil.ToFloat64(s.metrics.circuitState) != 1 {
		t.Fatalf("breaker should be tripped")
	}
	if prom_testutil.ToFloat64(s.metrics.dropped.WithLabelValues("memory_pressure")) != 1 {
		t.Fatalf("expected memory_pressure drop")
	}

	s.flush(1)
	require.NoError(t, s.Add(5, exemplar.Exemplar{Ts: 5}))
	if prom_testutil.ToFloat64(s.metrics.circuitState) != 0 {
		t.Fatalf("breaker should have reset")
	}
}

func TestJitterDeterministic(t *testing.T) {
	tw := 30 * time.Second
	j1 := jitter(123, tw)
	j2 := jitter(123, tw)
	if j1 != j2 {
		t.Fatalf("jitter should be deterministic")
	}
	if j1 < 0 || j1 > tw/10 {
		t.Fatalf("jitter out of bounds: %v", j1)
	}
}
