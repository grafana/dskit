package middleware

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// NewInvalidClusterValidations registers and returns a new counter metric server_request_invalid_cluster_validation_labels_total.
func NewInvalidClusterValidations(reg prometheus.Registerer) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "server_request_invalid_cluster_validation_labels_total",
		Help: "Number of requests received by server with invalid cluster validation label.",
	}, []string{"protocol", "method", "cluster_validation_label", "request_cluster_validation_label"})
}
