package middleware

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/instrument"
)

func TestThroughputMetricHistogram(t *testing.T) {
	tests := []struct {
		name     string
		sleep    bool
		header   string
		observed bool
	}{
		{"WithSleep", true, "unit=0, other_unit=2", true},
		{"WithoutSleep", false, "unit=0, other_unit=2", false},
		{"WithoutSleep", true, "", false},
		{"WithoutSleep", false, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			reg := prometheus.NewPedanticRegistry()
			i := NewInstrument(reg)

			wrap := i.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.sleep {
					time.Sleep(i.RequestCutoff)
				}
				w.Header().Set("Server-Timing", tt.header)
			}))

			req := httptest.NewRequest("GET", "/", nil)
			res := httptest.NewRecorder()

			wrap.ServeHTTP(res, req)

			output := ``
			if tt.observed {
				output = `
				# HELP request_throughput_unit Server throughput running requests.
				# TYPE request_throughput_unit histogram
				request_throughput_unit_bucket{cutoff_ms="100",method="GET",route="other",le="1"} 1
				request_throughput_unit_bucket{cutoff_ms="100",method="GET",route="other",le="5"} 1
				request_throughput_unit_bucket{cutoff_ms="100",method="GET",route="other",le="10"} 1
				request_throughput_unit_bucket{cutoff_ms="100",method="GET",route="other",le="+Inf"} 1
				request_throughput_unit_sum{cutoff_ms="100",method="GET",route="other"} 0
				request_throughput_unit_count{cutoff_ms="100",method="GET",route="other"} 1
			`
			}

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(output), "slow_request_throughput_"+i.ThroughputUnit))
		})
	}
}

func NewInstrument(registry *prometheus.Registry) Instrument {
	reg := promauto.With(registry)

	const metricsNativeHistogramFactor = 1.1
	const throughputUnit = "unit"
	const SlowRequestCutoff = 100 * time.Millisecond

	return Instrument{
		Duration: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "request_duration_seconds",
			Help:                            "Time (in seconds) spent serving HTTP requests.",
			Buckets:                         instrument.DefBuckets,
			NativeHistogramBucketFactor:     metricsNativeHistogramFactor,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, []string{"method", "route", "status_code", "ws"}),
		PerTenantDuration: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "per_tenant_request_duration_seconds",
			Help:                            "Time (in seconds) spent serving HTTP requests for a particular tenant.",
			Buckets:                         instrument.DefBuckets,
			NativeHistogramBucketFactor:     metricsNativeHistogramFactor,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, []string{"method", "route", "status_code", "ws", "tenant"}),
		RequestBodySize: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "request_message_bytes",
			Help:    "Size (in bytes) of messages received in the request.",
			Buckets: BodySizeBuckets,
		}, []string{"method", "route"}),
		ResponseBodySize: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "response_message_bytes",
			Help:    "Size (in bytes) of messages sent in response.",
			Buckets: BodySizeBuckets,
		}, []string{"method", "route"}),
		InflightRequests: reg.NewGaugeVec(prometheus.GaugeOpts{
			Name: "inflight_requests",
			Help: "Current number of inflight requests.",
		}, []string{"method", "route"}),
		RequestCutoff:  SlowRequestCutoff,
		ThroughputUnit: throughputUnit,
		RequestThroughput: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "request_throughput_" + throughputUnit,
			Help:                            "Server throughput running requests.",
			ConstLabels:                     prometheus.Labels{"cutoff_ms": strconv.FormatInt(SlowRequestCutoff.Milliseconds(), 10)},
			Buckets:                         []float64{1, 5, 10},
			NativeHistogramBucketFactor:     metricsNativeHistogramFactor,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, []string{"method", "route"}),
	}
}

func TestExtractValueFromMultiValueHeader(t *testing.T) {
	tests := []struct {
		header   string
		key      string
		expected int64
		err      bool
	}{
		{"key0=0, key1=1", "key0", 0, false},
		{"key0=0, key1=1", "key1", 1, false},
		{"key0=0, key1=1", "key2", 0, true},
		{"key0=1.0, key1=1", "key0", 1, false},
		{"foo", "foo", 0, true},
		{"foo=bar", "foo", 0, true},
		{"", "foo", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			value, err := extractValueFromMultiValueHeader(tt.header, tt.key)
			if (err != nil) != tt.err {
				t.Errorf("expected error: %v, got: %v", tt.err, err)
			}
			if value != tt.expected {
				t.Errorf("expected value: %d, got: %d", tt.expected, value)
			}
		})
	}
}
