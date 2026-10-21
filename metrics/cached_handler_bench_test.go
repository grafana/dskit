package metrics

import (
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func cachedHandlerBenchmarkRegistry(tenants int) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)
	histogram := factory.NewHistogramVec(prometheus.HistogramOpts{Name: "benchmark_request_bytes", Help: "Synthetic request sizes.", Buckets: []float64{math.Inf(1)}}, []string{"user", "handler"})
	counters := make([]*prometheus.CounterVec, 15)
	for i := range counters {
		counters[i] = factory.NewCounterVec(prometheus.CounterOpts{Name: fmt.Sprintf("benchmark_events_%02d_total", i), Help: "Synthetic tenant events."}, []string{"user"})
	}
	for i := 0; i < tenants; i++ {
		user := fmt.Sprintf("synthetic-%08d", i)
		histogram.WithLabelValues(user, "push").Observe(float64(1024 + i%10000))
		for j, counter := range counters {
			counter.WithLabelValues(user).Add(float64((i + 1) * (j + 1)))
		}
	}
	return reg
}

type metricsBenchmarkResponseWriter struct{ header http.Header }

func (w *metricsBenchmarkResponseWriter) Header() http.Header       { return w.header }
func (*metricsBenchmarkResponseWriter) WriteHeader(int)             {}
func (*metricsBenchmarkResponseWriter) Write(p []byte) (int, error) { return len(p), nil }

func BenchmarkCachedHandlerFor(b *testing.B) {
	for _, tenants := range []int{1000, 8000} {
		reg := cachedHandlerBenchmarkRegistry(tenants)
		for _, encoding := range []string{"identity", "gzip"} {
			for _, cached := range []bool{false, true} {
				b.Run(fmt.Sprintf("tenants=%d/%s/cached=%v", tenants, encoding, cached), func(b *testing.B) {
					opts := promhttp.HandlerOpts{EnableOpenMetrics: true}
					// Replay the same arrivals for both cases without timing RNG work.
					rng := rand.New(rand.NewPCG(1, 2))
					arrivals := make([]time.Time, b.N)
					arrival := time.Unix(0, 0)
					const interval = 3 * time.Second
					for i := range arrivals {
						arrival = arrival.Add(time.Duration(rng.Int64N(int64(2 * interval))))
						arrivals[i] = arrival
					}
					h := promhttp.HandlerFor(reg, opts)
					now := time.Unix(0, 0)
					if cached {
						c := CachedHandlerFor(reg, opts, 15*time.Second).(*cachedMetricsHandler)
						c.now = func() time.Time { return now }
						h = c
					}
					r := httptest.NewRequest("GET", "/metrics", nil)
					r.Header.Set("Accept", "text/plain;version=0.0.4")
					r.Header.Set("Accept-Encoding", encoding)
					w := &metricsBenchmarkResponseWriter{header: make(http.Header)}
					h.ServeHTTP(w, r)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						// Full-width jitter prevents alignment with the cache TTL.
						now = arrivals[i]
						h.ServeHTTP(w, r)
					}
				})
			}
		}
	}
}
