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
		testName string
		sleep    bool
		header   string
		observed bool
	}{
		{
			testName: "WithSleep",
			sleep:    true,
			header:   "unit;val=0, other_unit;val=2",
			observed: true,
		},
		{
			testName: "WithoutSleep",
			sleep:    false,
			header:   "unit;val=0, other_unit;val=2",
			observed: false,
		},
		{
			testName: "WithSleepEmptyHeader",
			sleep:    true,
			header:   "",
			observed: false,
		},
		{
			testName: "WithoutSleepEmptyHeader",
			sleep:    false,
			header:   "",
			observed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {

			reg := prometheus.NewPedanticRegistry()
			i := newInstrument(reg)

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

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(output), "request_throughput_"+i.ThroughputUnit))
		})
	}
}

func newInstrument(registry *prometheus.Registry) Instrument {
	reg := promauto.With(registry)

	const throughputUnit = "unit"
	const slowRequestCutoff = 100 * time.Millisecond

	return Instrument{
		Duration: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "request_duration_seconds",
			Help:    "Time (in seconds) spent serving HTTP requests.",
			Buckets: instrument.DefBuckets,
		}, []string{"method", "route", "status_code", "ws"}),
		PerTenantDuration: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "per_tenant_request_duration_seconds",
			Help:    "Time (in seconds) spent serving HTTP requests for a particular tenant.",
			Buckets: instrument.DefBuckets,
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
		RequestCutoff:  slowRequestCutoff,
		ThroughputUnit: throughputUnit,
		RequestThroughput: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "request_throughput_" + throughputUnit,
			Help:        "Server throughput running requests.",
			ConstLabels: prometheus.Labels{"cutoff_ms": strconv.FormatInt(slowRequestCutoff.Milliseconds(), 10)},
			Buckets:     []float64{1, 5, 10},
		}, []string{"method", "route"}),
	}
}

func TestExtractValueFromMultiValueHeader(t *testing.T) {
	tests := []struct {
		testName string
		header   string
		name     string
		key      string
		expected float64
		err      bool
	}{
		{
			testName: "ExistantKeyInName1",
			header:   "name0;key0=0.0;key1=1.1, name1;key0=1.1",
			name:     "name0",
			key:      "key0",
			expected: 0.0,
			err:      false,
		},
		{
			testName: "NonExistantName1",
			header:   "name0;key0=0.0;key1=1.1, name1;key0=1.1",
			name:     "name2",
			key:      "key0",
			expected: 0.0,
			err:      true,
		},
		{
			testName: "ExistantKeyInName2",
			header:   "name0;key0=0.0;key1=1.1, name1;key1=1.1",
			name:     "name0",
			key:      "key1",
			expected: 1.1,
			err:      false,
		},
		{
			testName: "NonExistantName2",
			header:   "name0;key0=0.0;key1=1.1, name1;key1=1.1",
			name:     "name2",
			key:      "key1",
			expected: 0.0,
			err:      true,
		},
		{
			testName: "StringInKey",
			header:   "name0;key0=str;key1=1.1",
			name:     "name0",
			key:      "key0",
			expected: 0,
			err:      true,
		},
		{
			testName: "EmptyHeader",
			header:   "",
			name:     "name0",
			key:      "key0",
			expected: 0,
			err:      true,
		},
		{
			testName: "IncorrectFormat",
			header:   "key0=0.0, key1=1.1",
			name:     "key0",
			key:      "key0",
			expected: 0,
			err:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			value, err := extractValueFromMultiValueHeader(tt.header, tt.name, tt.key)
			require.Equal(t, tt.err, err != nil, "expected error: %v, got: %v", tt.err, err)
			require.Equal(t, tt.expected, value, "expected value: %f, got: %f", tt.expected, value)
		})
	}
}
