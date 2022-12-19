package cache

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	opSet                 = "set"
	opGetMulti            = "getmulti"
	opDelete              = "delete"
	reasonMaxItemSize     = "max-item-size"
	reasonAsyncBufferFull = "async-buffer-full"
	reasonMalformedKey    = "malformed-key"
	reasonTimeout         = "timeout"
	reasonServerError     = "server-error"
	reasonNetworkError    = "network-error"
	reasonOther           = "other"
)

type clientMetrics struct {
	operations *prometheus.CounterVec
	failures   *prometheus.CounterVec
	skipped    *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	dataSize   *prometheus.HistogramVec
}

func newClientMetrics(reg prometheus.Registerer) *clientMetrics {
	cm := &clientMetrics{}

	cm.operations = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "operations_total",
		Help: "Total number of operations against cache.",
	}, []string{"operation"})
	cm.operations.WithLabelValues(opGetMulti)
	cm.operations.WithLabelValues(opSet)
	cm.operations.WithLabelValues(opDelete)

	cm.failures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "operation_failures_total",
		Help: "Total number of operations against cache that failed.",
	}, []string{"operation", "reason"})
	for _, op := range []string{opGetMulti, opSet, opDelete} {
		cm.failures.WithLabelValues(op, reasonTimeout)
		cm.failures.WithLabelValues(op, reasonMalformedKey)
		cm.failures.WithLabelValues(op, reasonServerError)
		cm.failures.WithLabelValues(op, reasonNetworkError)
		cm.failures.WithLabelValues(op, reasonOther)
	}

	cm.skipped = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "operation_skipped_total",
		Help: "Total number of operations against cache that have been skipped.",
	}, []string{"operation", "reason"})
	cm.skipped.WithLabelValues(opGetMulti, reasonMaxItemSize)
	cm.skipped.WithLabelValues(opSet, reasonMaxItemSize)
	cm.skipped.WithLabelValues(opSet, reasonAsyncBufferFull)

	cm.duration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "operation_duration_seconds",
		Help:    "Duration of operations against cache.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 3, 6, 10},
	}, []string{"operation"})
	cm.duration.WithLabelValues(opGetMulti)
	cm.duration.WithLabelValues(opSet)
	cm.duration.WithLabelValues(opDelete)

	cm.dataSize = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "operation_data_size_bytes",
		Help: "Tracks the size of the data stored in and fetched from cache.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
		},
	},
		[]string{"operation"},
	)
	cm.dataSize.WithLabelValues(opGetMulti)
	cm.dataSize.WithLabelValues(opSet)

	return cm
}
