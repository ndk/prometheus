package tsdb

import (
	"context"
	"sync"
	"testing"
	"time"

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/compression"
	"github.com/stretchr/testify/require"
)

func TestOOOExemplarAppender_FlushByTime(t *testing.T) {
	h, _ := newTestHead(t, 1000, compression.None, false)
	defer func() { require.NoError(t, h.Close()) }()

	cfg := config.ExemplarOutOfOrderConfig{
		TimeWindow:              model.Duration(100 * time.Millisecond),
		MaxBufferSizePerSeries:  10,
		MaxTotalBufferSizeBytes: 1 << 20,
	}
	s := NewOutOfOrderExemplarStorage(h, cfg)
	t.Cleanup(s.Stop)
	h.oooExemplarStorage = s

	lbls := labels.FromStrings("foo", "bar")
	app := h.Appender(context.Background())
	ref, err := app.Append(0, lbls, 200, 1)
	require.NoError(t, err)
	_, err = app.AppendExemplar(ref, lbls, exemplar.Exemplar{Value: 1, Ts: 200})
	require.NoError(t, err)
	app.Commit()

	app = h.Appender(context.Background())
	_, err = app.AppendExemplar(ref, lbls, exemplar.Exemplar{Value: 2, Ts: 100})
	require.NoError(t, err)
	app.Commit()

	m, err := labels.NewMatcher(labels.MatchEqual, "foo", "bar")
	require.NoError(t, err)
	eq, err := h.exemplars.ExemplarQuerier(context.Background())
	require.NoError(t, err)
	res, err := eq.Select(0, 300, []*labels.Matcher{m})
	require.NoError(t, err)
	if len(res[0].Exemplars) != 1 {
		t.Fatalf("expected only one exemplar before flush, got %d", len(res[0].Exemplars))
	}

	// Wait for two scheduler ticks to guarantee the buffer flushes.
	time.Sleep(3100 * time.Millisecond)

	eq, err = h.exemplars.ExemplarQuerier(context.Background())
	require.NoError(t, err)
	res, err = eq.Select(0, 300, []*labels.Matcher{m})
	require.NoError(t, err)
	if len(res[0].Exemplars) != 2 {
		t.Fatalf("expected exemplar to be flushed by time, got %d", len(res[0].Exemplars))
	}
}

func TestOOOExemplarAppender_FlushBySize(t *testing.T) {
	h, _ := newTestHead(t, 1000, compression.None, false)
	defer func() { require.NoError(t, h.Close()) }()

	cfg := config.ExemplarOutOfOrderConfig{
		TimeWindow:              model.Duration(30 * time.Second),
		MaxBufferSizePerSeries:  2,
		MaxTotalBufferSizeBytes: 1 << 20,
	}
	s := NewOutOfOrderExemplarStorage(h, cfg)
	t.Cleanup(s.Stop)
	h.oooExemplarStorage = s

	lbls := labels.FromStrings("foo", "bar")
	app := h.Appender(context.Background())
	ref, err := app.Append(0, lbls, 200, 1)
	require.NoError(t, err)
	_, err = app.AppendExemplar(ref, lbls, exemplar.Exemplar{Value: 1, Ts: 200})
	require.NoError(t, err)
	app.Commit()

	app = h.Appender(context.Background())
	_, err = app.AppendExemplar(ref, lbls, exemplar.Exemplar{Value: 2, Ts: 100})
	require.NoError(t, err)
	_, err = app.AppendExemplar(ref, lbls, exemplar.Exemplar{Value: 3, Ts: 90})
	require.NoError(t, err)
	app.Commit()

	// Allow background worker to flush the buffer triggered by size.
	time.Sleep(1 * time.Second)

	m, err := labels.NewMatcher(labels.MatchEqual, "foo", "bar")
	require.NoError(t, err)
	eq, err := h.exemplars.ExemplarQuerier(context.Background())
	require.NoError(t, err)
	res, err := eq.Select(0, 300, []*labels.Matcher{m})
	require.NoError(t, err)
	if len(res[0].Exemplars) != 3 {
		t.Fatalf("expected 3 exemplars flushed by size, got %d: %+v", len(res[0].Exemplars), res[0].Exemplars)
	}
}

func TestOOOConcurrentAppends(t *testing.T) {
	h, _ := newTestHead(t, 1000, compression.None, false)
	defer func() { require.NoError(t, h.Close()) }()

	cfg := config.ExemplarOutOfOrderConfig{
		TimeWindow:              model.Duration(100 * time.Millisecond),
		MaxBufferSizePerSeries:  5,
		MaxTotalBufferSizeBytes: 1 << 20,
	}
	s := NewOutOfOrderExemplarStorage(h, cfg)
	t.Cleanup(s.Stop)
	h.oooExemplarStorage = s

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := uint64(1)
			if i >= 5 {
				id = uint64(i + 1)
			}
			for j := 0; j < 5; j++ {
				require.NoError(t, s.Add(id, exemplar.Exemplar{Ts: int64(i*10 + j)}))
			}
		}(i)
	}
	wg.Wait()

	// Wait for all buffers to flush, up to a reasonable timeout.
	require.Eventually(t, func() bool {
		return prom_testutil.ToFloat64(s.metrics.bufferedBytes) == 0
	}, 5*time.Second, 100*time.Millisecond, "expected all buffers to flush")
}
