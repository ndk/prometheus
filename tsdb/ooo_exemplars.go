package tsdb

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"go.uber.org/atomic"
)

type oooSeriesBuffer struct {
	exemplars []exemplar.Exemplar
	created   time.Time
}

type oooExemplarMetrics struct {
	bufferedBytes prometheus.Gauge
	dropped       *prometheus.CounterVec
	circuitState  prometheus.Gauge
	received      prometheus.Counter
	flushDuration prometheus.Histogram
	queueLength   prometheus.Gauge
	staleness     prometheus.Histogram
}

type OutOfOrderExemplarStorage struct {
	head *Head
	cfg  config.ExemplarOutOfOrderConfig

	mtx        sync.RWMutex
	buffers    map[uint64]*oooSeriesBuffer
	flushQueue chan uint64

	metrics *oooExemplarMetrics

	breaker        atomic.Bool
	totalSizeBytes atomic.Int64

	lastQueueWarn time.Time

	stop     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

func newOooExemplarMetrics(reg prometheus.Registerer) *oooExemplarMetrics {
	m := &oooExemplarMetrics{
		bufferedBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_experimental_ooo_exemplar_buffer_bytes",
			Help: "Total size of buffered out-of-order exemplars in bytes.",
		}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "prometheus_tsdb_experimental_ooo_exemplar_dropped_total",
			Help: "Number of exemplars dropped in out-of-order exemplar subsystem.",
		}, []string{"reason"}),
		circuitState: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_experimental_ooo_exemplar_circuit_breaker_state",
			Help: "Circuit breaker state of the out-of-order exemplar subsystem (1 = open).",
		}),
		received: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_experimental_exemplars_out_of_order_received_total",
			Help: "Number of out-of-order exemplars received.",
		}),
		flushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "prometheus_tsdb_experimental_exemplars_out_of_order_flush_duration_seconds",
			Help:    "Duration of exemplar buffer flushes.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
		}),
		queueLength: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_experimental_exemplars_out_of_order_flush_queue_length",
			Help: "Number of series queued for flushing.",
		}),
		staleness: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "prometheus_tsdb_experimental_exemplars_staleness_seconds",
			Help:    "How stale exemplars were when flushed.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
		}),
	}
	if reg != nil {
		reg.MustRegister(
			m.bufferedBytes,
			m.dropped,
			m.circuitState,
			m.received,
			m.flushDuration,
			m.queueLength,
			m.staleness,
		)
	}
	return m
}

func NewOutOfOrderExemplarStorage(h *Head, cfg config.ExemplarOutOfOrderConfig) *OutOfOrderExemplarStorage {
	s := &OutOfOrderExemplarStorage{
		head:       h,
		cfg:        cfg,
		buffers:    make(map[uint64]*oooSeriesBuffer),
		flushQueue: make(chan uint64, 10000),
		metrics:    newOooExemplarMetrics(h.reg),
		stop:       make(chan struct{}),
	}

	s.metrics.circuitState.Set(0)
	s.metrics.bufferedBytes.Set(0)

	s.wg.Add(1)
	go s.scheduler()
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		s.wg.Add(1)
		go s.worker()
	}
	return s
}

func (s *OutOfOrderExemplarStorage) scheduler() {
	defer s.wg.Done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			now := time.Now()
			var ids []uint64
			s.mtx.RLock()
			for id, b := range s.buffers {
				if now.Sub(b.created) >= time.Duration(s.cfg.TimeWindow)-jitter(id, time.Duration(s.cfg.TimeWindow)) {
					ids = append(ids, id)
				}
			}
			s.mtx.RUnlock()
			for _, id := range ids {
				select {
				case s.flushQueue <- id:
					s.metrics.queueLength.Inc()
				default:
					if time.Since(s.lastQueueWarn) > 10*time.Second {
						s.head.logger.Warn("flush queue full", "seriesID", id)
						s.lastQueueWarn = time.Now()
					}
				}
			}
		case <-s.stop:
			return
		}
	}
}

func (s *OutOfOrderExemplarStorage) worker() {
	defer s.wg.Done()
	for id := range s.flushQueue {
		s.metrics.queueLength.Dec()
		s.flush(id)
	}
}

func (s *OutOfOrderExemplarStorage) flush(id uint64) {
	start := time.Now()
	s.mtx.Lock()
	buf, ok := s.buffers[id]
	if !ok {
		s.mtx.Unlock()
		return
	}
	exemplars := append([]exemplar.Exemplar(nil), buf.exemplars...)
	delete(s.buffers, id)
	s.mtx.Unlock()

	s.totalSizeBytes.Add(int64(-len(exemplars)) * 80)
	s.metrics.bufferedBytes.Set(float64(s.totalSizeBytes.Load()))

	if s.breaker.Load() && s.totalSizeBytes.Load() < int64(float64(s.cfg.MaxTotalBufferSizeBytes)*0.85) {
		s.breaker.Store(false)
		s.metrics.circuitState.Set(0)
	}

	ms := s.head.series.getByID(chunks.HeadSeriesRef(id))
	if ms == nil {
		s.metrics.dropped.WithLabelValues("series_deleted").Inc()
		return
	}

	valid := make([]exemplar.Exemplar, 0, len(exemplars))
	for _, ex := range exemplars {
		if err := s.head.exemplars.ValidateExemplar(ms.labels(), ex); err != nil {
			if errors.Is(err, storage.ErrDuplicateExemplar) {
				s.metrics.dropped.WithLabelValues("duplicate").Inc()
				continue
			}
			if !errors.Is(err, storage.ErrOutOfOrderExemplar) {
				// Drop exemplars with any other validation error.
				continue
			}
		}
		s.metrics.staleness.Observe(time.Since(time.UnixMilli(ex.Ts)).Seconds())
		valid = append(valid, ex)
	}

	sort.Slice(valid, func(i, j int) bool { return valid[i].Ts < valid[j].Ts })
	s.head.commitBufferedExemplars(id, valid)
	s.metrics.flushDuration.Observe(time.Since(start).Seconds())
}

func (s *OutOfOrderExemplarStorage) Add(id uint64, e exemplar.Exemplar) error {
	s.metrics.received.Inc()
	if s.breaker.Load() {
		if s.totalSizeBytes.Load() < int64(float64(s.cfg.MaxTotalBufferSizeBytes)*0.85) {
			s.breaker.Store(false)
			s.metrics.circuitState.Set(0)
		} else {
			s.metrics.dropped.WithLabelValues("memory_pressure").Inc()
			return nil
		}
	}

	if s.totalSizeBytes.Load() >= s.cfg.MaxTotalBufferSizeBytes {
		s.breaker.Store(true)
		s.metrics.circuitState.Set(1)
		s.metrics.dropped.WithLabelValues("memory_pressure").Inc()
		return nil
	}

	s.mtx.Lock()
	buf, ok := s.buffers[id]
	if !ok {
		buf = &oooSeriesBuffer{created: time.Now()}
		s.buffers[id] = buf
	}
	buf.exemplars = append(buf.exemplars, e)
	if len(buf.exemplars) >= s.cfg.MaxBufferSizePerSeries {
		select {
		case s.flushQueue <- id:
			s.metrics.queueLength.Inc()
		default:
			s.head.logger.Warn("flush queue full", "seriesID", id)
		}
	}
	s.totalSizeBytes.Add(80)
	s.metrics.bufferedBytes.Set(float64(s.totalSizeBytes.Load()))
	s.mtx.Unlock()
	return nil
}

// Stop gracefully shuts down the exemplar storage ensuring all buffered
// exemplars are flushed before returning.
func (s *OutOfOrderExemplarStorage) Stop() {
	s.stopOnce.Do(func() {
		// Stop the scheduler so no new flush tasks are enqueued.
		close(s.stop)

		// Enqueue all remaining buffers for flushing.
		s.mtx.RLock()
		for id := range s.buffers {
			select {
			case s.flushQueue <- id:
				s.metrics.queueLength.Inc()
			default:
				s.head.logger.Warn("flush queue full during shutdown", "seriesID", id)
			}
		}
		s.mtx.RUnlock()

		// Signal workers that no more tasks will arrive.
		close(s.flushQueue)

		// Wait for all workers and the scheduler to exit.
		s.wg.Wait()
	})
}

func jitter(id uint64, tw time.Duration) time.Duration {
	h := fnv.New64()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], id)
	h.Write(b[:])
	return (tw / 100) * time.Duration(h.Sum64()%10)
}
