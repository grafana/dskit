package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func scrapeCachedHandler(h http.Handler, accept string, encoding ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	r.Header.Set("Accept", accept)
	r.Header["Accept-Encoding"] = encoding
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestCachedHandlerFor(t *testing.T) {
	for _, created := range []bool{false, true} {
		t.Run(fmt.Sprintf("created=%v", created), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			gauge := promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "test_value", Help: "A changing value."})
			counter := promauto.With(reg).NewCounter(prometheus.CounterOpts{Name: "test_events_total", Help: "Events."})
			counter.Inc()
			var gathers atomic.Int64
			gatherer := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { gathers.Inc(); return reg.Gather() })
			opts := promhttp.HandlerOpts{EnableOpenMetrics: true, EnableOpenMetricsTextCreatedSamples: created, ProcessStartTime: time.Unix(1234, 0)}
			h := CachedHandlerFor(gatherer, opts, 15*time.Second).(*cachedMetricsHandler)
			now := time.Unix(0, 0)
			h.now = func() time.Time { return now }
			plain := promhttp.HandlerFor(reg, opts)
			for _, accept := range []string{"text/plain;version=0.0.4", "application/openmetrics-text;version=1.0.0", "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited", "text/plain;version=0.0.4;escaping=values"} {
				for _, encoding := range []string{"identity", "gzip"} {
					t.Run(accept+"/"+encoding, func(t *testing.T) {
						gauge.Set(1)
						before := gathers.Load()
						baseline := scrapeCachedHandler(plain, accept, encoding)
						first := scrapeCachedHandler(h, accept, encoding)
						require.Equal(t, baseline.Code, first.Code)
						require.Equal(t, baseline.Header(), first.Header())
						require.Equal(t, baseline.Body.Bytes(), first.Body.Bytes())
						require.Equal(t, before+1, gathers.Load())
						gauge.Set(2)
						now = now.Add(14 * time.Second)
						hit := scrapeCachedHandler(h, accept, encoding)
						require.Equal(t, first.Body.Bytes(), hit.Body.Bytes())
						require.Equal(t, before+1, gathers.Load())
						// A caller may mutate its own response headers after the handler returns.
						hit.Header()["Vary"][0] = "modified"
						require.Equal(t, first.Header(), scrapeCachedHandler(h, accept, encoding).Header())
						now = now.Add(time.Second)
						refreshed := scrapeCachedHandler(h, accept, encoding)
						require.Equal(t, before+2, gathers.Load())
						require.Equal(t, scrapeCachedHandler(plain, accept, encoding).Body.Bytes(), refreshed.Body.Bytes())
					})
				}
			}
		})
	}
}

func TestCachedHandlerForDisabled(t *testing.T) {
	for _, duration := range []time.Duration{0, -time.Second} {
		t.Run(duration.String(), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			gauge := promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "test_value", Help: "Value."})
			h := CachedHandlerFor(reg, promhttp.HandlerOpts{}, duration)
			gauge.Set(1)
			require.Contains(t, scrapeCachedHandler(h, "").Body.String(), "test_value 1\n")
			gauge.Set(2)
			require.Contains(t, scrapeCachedHandler(h, "").Body.String(), "test_value 2\n")
		})
	}
}

func TestCachedHandlerForBypass(t *testing.T) {
	reg := prometheus.NewRegistry()
	gauge := promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "test_value", Help: "Value."})
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "test_other", Help: "Another value."})
	h := CachedHandlerFor(reg, promhttp.HandlerOpts{}, time.Minute)
	gauge.Set(1)
	first := scrapeCachedHandler(h, "")
	gauge.Set(2)
	for _, tc := range []struct{ method, path string }{{"GET", "/metrics?name[]=test_value"}, {"GET", "/metrics?anything=else"}, {"HEAD", "/metrics"}, {"POST", "/metrics"}} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			baseline := httptest.NewRecorder()
			promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(baseline, r)
			require.Equal(t, baseline.Code, w.Code)
			require.Equal(t, baseline.Header(), w.Header())
			require.Equal(t, baseline.Body.String(), w.Body.String())
		})
	}
	require.Equal(t, first.Body.String(), scrapeCachedHandler(h, "").Body.String())
}

func TestCachedHandlerForMultipleEncodingHeaders(t *testing.T) {
	reg := prometheus.NewRegistry()
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "test_value", Help: "Value."})
	opts := promhttp.HandlerOpts{}
	h := CachedHandlerFor(reg, opts, time.Minute)
	for _, encodings := range [][]string{{"gzip;q=0"}, {"gzip;q=0", "gzip;q=1"}, {"gzip;q=0"}} {
		baseline := scrapeCachedHandler(promhttp.HandlerFor(reg, opts), "", encodings...)
		cached := scrapeCachedHandler(h, "", encodings...)
		require.Equal(t, baseline.Header(), cached.Header())
		require.Equal(t, baseline.Body.Bytes(), cached.Body.Bytes())
	}
}

func TestCachedHandlerForErrors(t *testing.T) {
	for _, handling := range []promhttp.HandlerErrorHandling{promhttp.HTTPErrorOnError, promhttp.ContinueOnError} {
		t.Run(fmt.Sprint(handling), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "test_value", Help: "Value."})
			gathers := 0
			fail := true
			gatherer := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
				gathers++
				families, err := reg.Gather()
				if fail {
					return families, errors.New("gather failed")
				}
				return families, err
			})
			var logs bytes.Buffer
			h := CachedHandlerFor(gatherer, promhttp.HandlerOpts{ErrorHandling: handling, ErrorLog: log.New(&logs, "", 0)}, time.Minute)
			for i := 0; i < 2; i++ {
				w := scrapeCachedHandler(h, "")
				if handling == promhttp.HTTPErrorOnError {
					require.Equal(t, http.StatusInternalServerError, w.Code)
					require.Contains(t, w.Header().Get("Content-Type"), "text/plain")
				} else {
					require.Equal(t, http.StatusOK, w.Code)
					require.Contains(t, w.Body.String(), "test_value")
				}
			}
			if handling == promhttp.ContinueOnError {
				// Partial responses are cached, as every uncached scrape would return them.
				require.Equal(t, 1, gathers)
				require.Equal(t, 1, strings.Count(logs.String(), "gather failed"))
				return
			}
			require.Equal(t, 2, gathers)
			require.Equal(t, 2, strings.Count(logs.String(), "gather failed"))
			fail = false
			require.Equal(t, http.StatusOK, scrapeCachedHandler(h, "").Code)
			require.Equal(t, http.StatusOK, scrapeCachedHandler(h, "").Code)
			require.Equal(t, 3, gathers)
		})
	}
}

func TestCachedHandlerForEncodingError(t *testing.T) {
	valid := &dto.MetricFamily{Name: strptr("a_valid"), Help: strptr("Valid metric."), Type: dto.MetricType_GAUGE.Enum(), Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: floatptr(1)}}}}
	invalid := &dto.MetricFamily{Name: strptr("z_invalid"), Help: strptr("Invalid metric."), Type: dto.MetricType_COUNTER.Enum(), Metric: []*dto.Metric{{}}}
	gathers := 0
	reg := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { gathers++; return []*dto.MetricFamily{valid, invalid}, nil })
	h := CachedHandlerFor(reg, promhttp.HandlerOpts{}, time.Minute)
	for i := 0; i < 2; i++ {
		require.Equal(t, http.StatusOK, scrapeCachedHandler(h, "text/plain").Code)
	}
	require.Equal(t, 2, gathers)
}

func strptr(s string) *string     { return &s }
func floatptr(f float64) *float64 { return &f }

func TestCachedHandlerForEmptyRegistry(t *testing.T) {
	var gathers atomic.Int64
	h := CachedHandlerFor(prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { gathers.Inc(); return nil, nil }), promhttp.HandlerOpts{}, time.Minute)
	for i := 0; i < 2; i++ {
		w := scrapeCachedHandler(h, "")
		require.Equal(t, http.StatusOK, w.Code)
		require.Empty(t, w.Body.String())
	}
	require.Equal(t, int64(1), gathers.Load())
}

func TestCachedHandlerForConcurrency(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var gathers atomic.Int64
	reg := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { gathers.Inc(); close(entered); <-release; return nil, nil })
	h := CachedHandlerFor(reg, promhttp.HandlerOpts{}, time.Minute)
	first := make(chan struct{})
	go func() { defer close(first); scrapeCachedHandler(h, "") }()
	<-entered
	ctx, cancel := context.WithCancel(t.Context())
	canceled := make(chan struct{})
	go func() {
		defer close(canceled)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(ctx, "GET", "/metrics", nil))
	}()
	cancel()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("canceled waiter blocked")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); scrapeCachedHandler(h, "") }()
	}
	close(release)
	<-first
	wg.Wait()
	require.Equal(t, int64(1), gathers.Load())
}

func TestCachedHandlerForTimeout(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var gathers atomic.Int64
	reg := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		if gathers.Inc() == 1 {
			close(entered)
		}
		<-release
		return nil, nil
	})
	h := CachedHandlerFor(reg, promhttp.HandlerOpts{Timeout: 20 * time.Millisecond}, time.Minute)
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- scrapeCachedHandler(h, "") }()
	<-entered
	w := <-result
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), "Exceeded configured timeout")
	// Later requests time out waiting for the same refresh instead of starting another.
	require.Equal(t, http.StatusServiceUnavailable, scrapeCachedHandler(h, "").Code)
	require.Equal(t, int64(1), gathers.Load())
	close(release)
	require.Eventually(t, func() bool { return scrapeCachedHandler(h, "").Code == http.StatusOK }, time.Second, time.Millisecond)
	// The timed-out refresh still populated the cache.
	require.Equal(t, int64(1), gathers.Load())
	// Bypassed requests keep promhttp's timeout.
	release = make(chan struct{})
	r := httptest.NewRequest(http.MethodHead, "/metrics", nil)
	bypassed := httptest.NewRecorder()
	h.ServeHTTP(bypassed, r)
	require.Equal(t, http.StatusServiceUnavailable, bypassed.Code)
	close(release)
}

func TestCachedHandlerForBoundedVariants(t *testing.T) {
	h := CachedHandlerFor(prometheus.NewRegistry(), promhttp.HandlerOpts{}, time.Minute).(*cachedMetricsHandler)
	for i := 0; i < 100; i++ {
		scrapeCachedHandler(h, fmt.Sprintf("text/plain;unused=%d", i))
	}
	require.Equal(t, metricsResponseCacheSize, h.responses.Len())
}

func TestCachedHandlerForSlowCollection(t *testing.T) {
	var mu sync.Mutex
	now := time.Unix(0, 0)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var gathers atomic.Int64
	reg := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		gathers.Inc()
		entered <- struct{}{}
		<-release
		mu.Lock()
		now = now.Add(time.Minute)
		mu.Unlock()
		return nil, nil
	})
	h := CachedHandlerFor(reg, promhttp.HandlerOpts{}, time.Second).(*cachedMetricsHandler)
	h.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	var wg sync.WaitGroup
	codes := make(chan int, 10)
	scrape := func() { defer wg.Done(); codes <- scrapeCachedHandler(h, "").Code }
	wg.Add(1)
	go scrape()
	<-entered
	for i := 0; i < 9; i++ {
		wg.Add(1)
		go scrape()
	}
	// Give the waiters time to join the in-flight refresh.
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	close(codes)
	for code := range codes {
		require.Equal(t, http.StatusOK, code)
	}
	// Requests arriving during a refresh share it even if it outlives the TTL.
	require.Equal(t, int64(1), gathers.Load())
	require.Zero(t, h.responses.Len())
}

func TestCachedHandlerForIndependentVariants(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	reg := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		blocked := false
		once.Do(func() { blocked = true; close(entered) })
		if blocked {
			<-release
		}
		return nil, nil
	})
	h := CachedHandlerFor(reg, promhttp.HandlerOpts{}, time.Minute)
	slow := make(chan struct{})
	go func() { defer close(slow); scrapeCachedHandler(h, "text/plain") }()
	<-entered
	// A different representation does not wait for an unrelated refresh.
	require.Equal(t, http.StatusOK, scrapeCachedHandler(h, "application/openmetrics-text").Code)
	close(release)
	<-slow
}

func TestCachedHandlerForErrorsAttributedPerRefresh(t *testing.T) {
	reg := prometheus.NewRegistry()
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "test_value", Help: "Value."})
	invalid := &dto.MetricFamily{Name: strptr("z_invalid"), Help: strptr("Invalid metric."), Type: dto.MetricType_COUNTER.Enum(), Metric: []*dto.Metric{{}}}
	var gathers atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	gatherer := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		families, err := reg.Gather()
		if gathers.Inc() == 1 {
			close(entered)
			<-release
			return families, err
		}
		return append(families, invalid), err
	})
	h := CachedHandlerFor(gatherer, promhttp.HandlerOpts{}, time.Minute)
	cached := make(chan struct{})
	go func() { defer close(cached); scrapeCachedHandler(h, "") }()
	<-entered
	// A bypassed request logging an encoding error must not prevent caching the refresh.
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/metrics?x=y", nil))
	close(release)
	<-cached
	scrapeCachedHandler(h, "")
	require.Equal(t, int64(2), gathers.Load())
}

func TestCachedHandlerForPanic(t *testing.T) {
	fail := true
	gatherer := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		if fail {
			return nil, errors.New("gather failed")
		}
		return nil, nil
	})
	h := CachedHandlerFor(gatherer, promhttp.HandlerOpts{ErrorHandling: promhttp.PanicOnError}, time.Minute)
	require.PanicsWithError(t, "gather failed", func() { scrapeCachedHandler(h, "") })
	fail = false
	require.Equal(t, http.StatusOK, scrapeCachedHandler(h, "").Code)
}

func TestCachedHandlerForPreservesMiddlewareHeaders(t *testing.T) {
	reg := prometheus.NewRegistry()
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "test_value", Help: "Value."})
	opts := promhttp.HandlerOpts{EnableOpenMetrics: true}
	cached := CachedHandlerFor(reg, opts, time.Minute)
	baseline := promhttp.HandlerFor(reg, opts)
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		want, got := httptest.NewRecorder(), httptest.NewRecorder()
		want.Header().Add("Vary", "Origin")
		got.Header().Add("Vary", "Origin")
		want.Header().Set("X-Middleware", "retained")
		got.Header().Set("X-Middleware", "retained")
		baseline.ServeHTTP(want, r)
		cached.ServeHTTP(got, r)
		require.Equal(t, want.Header(), got.Header())
		require.Equal(t, want.Body.Bytes(), got.Body.Bytes())
	}
}
