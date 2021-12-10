package kubernetes

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	prefix string

	casAttempts  prometheus.Counter
	casSuccesses prometheus.Counter
	casFailures  prometheus.Counter
}

func newMetrics(registerer prometheus.Registerer) *metrics {
	m := metrics{
		prefix: "kubernetes_ring_",
	}

	registerer = prometheus.WrapRegistererWithPrefix(m.prefix, registerer)

	m.casAttempts = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Name: "cas_attempt_total",
		Help: "Attempted CAS operations",
	})

	m.casSuccesses = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Name: "cas_success_total",
		Help: "Successful CAS operations",
	})

	m.casFailures = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Name: "cas_failure_total",
		Help: "Failed CAS operations",
	})

	return &m
}
