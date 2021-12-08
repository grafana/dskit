package kubernetes

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	casAttempts  prometheus.Counter
	casSuccesses prometheus.Counter
	casFailures  prometheus.Counter
}

func newMetrics(namespace string, registerer prometheus.Registerer) *metrics {
	const subsystem = "kubernetes_ring"
	m := metrics{}

	m.casAttempts = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cas_attempt_total",
		Help:      "Attempted CAS operations",
	})

	m.casSuccesses = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cas_success_total",
		Help:      "Successful CAS operations",
	})

	m.casFailures = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cas_failure_total",
		Help:      "Failed CAS operations",
	})

	return &m
}
