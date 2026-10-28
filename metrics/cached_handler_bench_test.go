package metrics

import (
	"cmp"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/atomic"
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

// Each scraper keeps its initial phase so periodic scrapes cannot drift against the TTL.
func BenchmarkCachedHandlerForPeriodicScrapers(b *testing.B) {
	const interval = 15 * time.Second
	for _, tenants := range []int{1000, 8000} {
		reg := cachedHandlerBenchmarkRegistry(tenants)
		for _, encoding := range []string{"identity", "gzip"} {
			for _, cached := range []bool{false, true} {
				b.Run(fmt.Sprintf("tenants=%d/%s/cached=%v", tenants, encoding, cached), func(b *testing.B) {
					var gathers atomic.Int64
					gatherer := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { gathers.Inc(); return reg.Gather() })
					opts := promhttp.HandlerOpts{EnableOpenMetrics: true}
					origin := time.Unix(0, 0)
					now := origin
					h := promhttp.HandlerFor(gatherer, opts)
					if cached {
						c := CachedHandlerFor(gatherer, opts, 14*time.Second).(*cachedMetricsHandler)
						c.now = func() time.Time { return now }
						h = c
					}
					type scraper struct {
						phase   time.Duration
						request *http.Request
					}
					scrapers := make([]scraper, 5)
					rng := rand.New(rand.NewPCG(1, 2))
					for i := range scrapers {
						path := "/metrics"
						if i < 2 {
							path += "?name[]=benchmark_events_00_total&name[]=benchmark_events_01_total"
						}
						request := httptest.NewRequest(http.MethodGet, path, nil)
						request.Header.Set("Accept", "text/plain;version=0.0.4")
						request.Header.Set("Accept-Encoding", encoding)
						scrapers[i] = scraper{phase: time.Duration(rng.Int64N(int64(interval))), request: request}
					}
					slices.SortFunc(scrapers, func(a, b scraper) int { return cmp.Compare(a.phase, b.phase) })
					w := &metricsBenchmarkResponseWriter{header: make(http.Header)}
					// Warm up one complete period, keeping measured arrivals on the same phases.
					for _, s := range scrapers {
						now = origin.Add(s.phase)
						h.ServeHTTP(w, s.request)
					}
					before := gathers.Load()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						s := scrapers[i%len(scrapers)]
						now = origin.Add(time.Duration(i/len(scrapers)+1)*interval + s.phase)
						h.ServeHTTP(w, s.request)
					}
					b.StopTimer()
					b.ReportMetric(float64(gathers.Load()-before)/float64(b.N), "gathers/op")
				})
			}
		}
	}
}
