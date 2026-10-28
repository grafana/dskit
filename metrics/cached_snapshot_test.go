package metrics

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	_ "github.com/prometheus/client_golang/prometheus/promhttp/zstd"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func scrapeMetricsPath(h http.Handler, path, accept, encoding string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.Header.Set("Accept", accept)
	r.Header.Set("Accept-Encoding", encoding)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func metricsResponseBody(t *testing.T, w *httptest.ResponseRecorder) []byte {
	t.Helper()
	switch w.Header().Get("Content-Encoding") {
	case "gzip":
		r, err := gzip.NewReader(bytes.NewReader(w.Body.Bytes()))
		require.NoError(t, err)
		defer r.Close()
		body, err := io.ReadAll(r)
		require.NoError(t, err)
		return body
	case "zstd":
		r, err := zstd.NewReader(bytes.NewReader(w.Body.Bytes()))
		require.NoError(t, err)
		defer r.Close()
		body, err := io.ReadAll(r)
		require.NoError(t, err)
		return body
	default:
		return w.Body.Bytes()
	}
}

func TestCachedHandlerForFilteredSnapshot(t *testing.T) {
	for _, filteredFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("filtered_first=%v", filteredFirst), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			first := promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "first", Help: "First."})
			second := promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "second", Help: "Second."})
			first.Set(1)
			second.Set(2)
			var gathers atomic.Int64
			h := CachedHandlerFor(prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { gathers.Inc(); return reg.Gather() }), promhttp.HandlerOpts{}, time.Minute).(*cachedMetricsHandler)
			now := time.Unix(0, 0)
			h.now = func() time.Time { return now }
			path := "/metrics"
			if filteredFirst {
				path += "?name[]=first"
			}
			require.Equal(t, http.StatusOK, scrapeMetricsPath(h, path, "", "").Code)
			first.Set(10)
			second.Set(20)
			require.Contains(t, scrapeMetricsPath(h, "/metrics?name[]=first", "", "").Body.String(), "first 1\n")
			require.Contains(t, scrapeMetricsPath(h, "/metrics?name[]=second", "", "").Body.String(), "second 2\n")
			require.Contains(t, scrapeMetricsPath(h, "/metrics", "", "").Body.String(), "first 1\n")
			require.Contains(t, scrapeMetricsPath(h, "/metrics", "", "").Body.String(), "second 2\n")
			require.Equal(t, int64(1), gathers.Load())
			require.Equal(t, 1, h.responses.Len())
			now = now.Add(time.Minute)
			require.Contains(t, scrapeMetricsPath(h, "/metrics?name[]=second", "", "").Body.String(), "second 20\n")
			require.Contains(t, scrapeMetricsPath(h, "/metrics", "", "").Body.String(), "first 10\n")
			require.Equal(t, int64(2), gathers.Load())
		})
	}
}

func TestCachedHandlerForFilteredFormats(t *testing.T) {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)
	factory.NewGauge(prometheus.GaugeOpts{Name: "a_gauge", Help: "A gauge."}).Set(3)
	factory.NewCounter(prometheus.CounterOpts{Name: "b_counter_total", Help: "A counter."}).Add(4)
	factory.NewHistogram(prometheus.HistogramOpts{Name: "c_histogram", Help: "A histogram."}).Observe(1)
	factory.NewSummary(prometheus.SummaryOpts{Name: "d_summary", Help: "A summary."}).Observe(2)
	factory.NewHistogram(prometheus.HistogramOpts{Name: "e_native", Help: "A native histogram.", NativeHistogramBucketFactor: 1.1}).Observe(3)
	for _, created := range []bool{false, true} {
		opts := promhttp.HandlerOpts{EnableOpenMetrics: true, EnableOpenMetricsTextCreatedSamples: created, ProcessStartTime: time.Unix(123, 0), AcceptedFormats: append(expfmt.DefaultOpenMetricsAcceptedFormats(), expfmt.FmtOpenMetrics_2_0_0)}
		var gathers atomic.Int64
		h := CachedHandlerFor(prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { gathers.Inc(); return reg.Gather() }), opts, time.Minute)
		plain := promhttp.HandlerFor(reg, opts)
		for _, format := range []string{string(expfmt.FmtText), string(expfmt.FmtOpenMetrics_1_0_0), string(expfmt.FmtOpenMetrics_2_0_0), string(expfmt.FmtProtoDelim), string(expfmt.FmtProtoText), string(expfmt.FmtProtoCompact), "text/plain;version=0.0.4;escaping=values"} {
			for _, encoding := range []string{"identity", "gzip", "zstd"} {
				for _, query := range []string{"", "name[]=b_counter_total&name[]=c_histogram", "name[]=d_summary&name[]=a_gauge&name[]=a_gauge", "name[]=missing", "name[]="} {
					t.Run(fmt.Sprintf("%v/%s/%s/%s", created, format, encoding, query), func(t *testing.T) {
						path := "/metrics"
						if query != "" {
							path += "?" + query
						}
						want := scrapeMetricsPath(plain, path, format, encoding)
						got := scrapeMetricsPath(h, path, format, encoding)
						require.Equal(t, want.Code, got.Code)
						require.Equal(t, want.Header(), got.Header())
						require.Equal(t, metricsResponseBody(t, want), metricsResponseBody(t, got))
						if strings.Contains(got.Header().Get("Content-Type"), "openmetrics") {
							require.Equal(t, 1, bytes.Count(metricsResponseBody(t, got), []byte("# EOF\n")))
						}
					})
				}
			}
		}
		require.Equal(t, int64(1), gathers.Load(), "formats, compression and name selections must share a gather")
	}
}

func TestCachedHandlerForFilterCombinationsAreNotCached(t *testing.T) {
	var gathers atomic.Int64
	reg := prometheus.NewRegistry()
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "existing", Help: "Existing."}).Set(1)
	h := CachedHandlerFor(prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { gathers.Inc(); return reg.Gather() }), promhttp.HandlerOpts{}, time.Minute).(*cachedMetricsHandler)
	for i := 0; i < 100; i++ {
		path := "/metrics?name[]=existing&name[]=" + url.QueryEscape(fmt.Sprintf("unknown_%d", i))
		require.Contains(t, scrapeMetricsPath(h, path, "", "gzip").Header().Get("Content-Encoding"), "gzip")
	}
	require.Equal(t, int64(1), gathers.Load())
	require.Equal(t, 1, h.responses.Len())
}

func TestCachedHandlerForSnapshotOwnsMetrics(t *testing.T) {
	family := &dto.MetricFamily{Name: strptr("mutable"), Type: dto.MetricType_GAUGE.Enum(), Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: floatptr(1)}}}}
	h := CachedHandlerFor(prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return []*dto.MetricFamily{family}, nil }), promhttp.HandlerOpts{EnableOpenMetrics: true}, time.Minute)
	require.Contains(t, scrapeMetricsPath(h, "/metrics", string(expfmt.FmtText), "").Body.String(), "mutable 1\n")
	family.Metric[0].Gauge.Value = floatptr(2)
	// Encoding a new representation must not observe a collector's reused protobuf.
	require.Contains(t, scrapeMetricsPath(h, "/metrics?name[]=mutable", string(expfmt.FmtOpenMetrics_1_0_0), "").Body.String(), "mutable 1.0\n")
}

func TestCachedHandlerForConcurrentFilters(t *testing.T) {
	var gathers atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	reg := prometheus.NewRegistry()
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "selected", Help: "Selected."}).Set(1)
	h := CachedHandlerFor(prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		if gathers.Inc() == 1 {
			close(entered)
		}
		<-release
		return reg.Gather()
	}), promhttp.HandlerOpts{EnableOpenMetrics: true}, time.Minute)
	results := make(chan *httptest.ResponseRecorder, 12)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			encoding := "identity"
			if i%2 == 0 {
				encoding = "gzip"
			}
			w := scrapeMetricsPath(h, "/metrics?name[]=selected", "text/plain", encoding)
			results <- w
		}(i)
	}
	<-entered
	close(release)
	wg.Wait()
	close(results)
	for w := range results {
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, string(metricsResponseBody(t, w)), "selected 1\n")
	}
	require.Equal(t, int64(1), gathers.Load())
}

// The network can fail after a cache hit, independently of snapshot collection.
type failingMetricsWriter struct{ header http.Header }

func (w failingMetricsWriter) Header() http.Header     { return w.header }
func (failingMetricsWriter) WriteHeader(int)           {}
func (failingMetricsWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestCachedHandlerForFilteredWriteError(t *testing.T) {
	for _, handling := range []promhttp.HandlerErrorHandling{promhttp.HTTPErrorOnError, promhttp.ContinueOnError, promhttp.PanicOnError} {
		t.Run(fmt.Sprint(handling), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "selected", Help: "Selected."})
			var logs bytes.Buffer
			h := CachedHandlerFor(reg, promhttp.HandlerOpts{Registry: reg, ErrorLog: log.New(&logs, "", 0), ErrorHandling: handling}, time.Minute)
			scrapeMetricsPath(h, "/metrics", "", "")
			scrape := func() {
				h.ServeHTTP(failingMetricsWriter{header: make(http.Header)}, httptest.NewRequest(http.MethodGet, "/metrics?name[]=selected", nil))
			}
			if handling == promhttp.PanicOnError {
				require.PanicsWithError(t, "write failed", scrape)
			} else {
				require.NotPanics(t, scrape)
			}
			require.Contains(t, logs.String(), "error encoding and sending metric family: write failed")
			families, err := reg.Gather()
			require.NoError(t, err)
			for _, family := range families {
				if family.GetName() == "promhttp_metric_handler_errors_total" {
					for _, metric := range family.Metric {
						if metric.Label[0].GetValue() == "encoding" {
							require.Equal(t, float64(1), metric.Counter.GetValue())
						}
					}
				}
			}
		})
	}
}

func TestCachedHandlerForFilteredUnselectedEncodingError(t *testing.T) {
	reg := prometheus.NewRegistry()
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "selected", Help: "Selected."})
	invalid := &dto.MetricFamily{Name: strptr("z_invalid"), Type: dto.MetricType_COUNTER.Enum(), Metric: []*dto.Metric{{}}}
	gatherer := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		families, err := reg.Gather()
		return append(families, invalid), err
	})
	var logs bytes.Buffer
	h := CachedHandlerFor(gatherer, promhttp.HandlerOpts{ErrorHandling: promhttp.PanicOnError, ErrorLog: log.New(&logs, "", 0)}, time.Minute)
	var w *httptest.ResponseRecorder
	require.NotPanics(t, func() { w = scrapeMetricsPath(h, "/metrics?name[]=selected", "", "") })
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "selected 0\n")
	require.Empty(t, logs.String())
}

func TestCachedHandlerForFilteredSnapshotExpiryAcrossFormats(t *testing.T) {
	reg := prometheus.NewRegistry()
	value := promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "selected", Help: "Selected."})
	var gathers atomic.Int64
	h := CachedHandlerFor(prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { gathers.Inc(); return reg.Gather() }), promhttp.HandlerOpts{EnableOpenMetrics: true}, time.Minute).(*cachedMetricsHandler)
	now := time.Unix(0, 0)
	h.now = func() time.Time { return now }
	scrapeMetricsPath(h, "/metrics", "text/plain", "")
	now = now.Add(59 * time.Second)
	value.Set(1)
	w := scrapeMetricsPath(h, "/metrics?name[]=selected", "application/openmetrics-text;version=1.0.0", "")
	require.Contains(t, w.Body.String(), "selected 0.0\n")
	now = now.Add(time.Second)
	w = scrapeMetricsPath(h, "/metrics?name[]=selected", "application/openmetrics-text;version=1.0.0", "")
	require.Contains(t, w.Body.String(), "selected 1.0\n")
	require.Equal(t, int64(2), gathers.Load())
}

func TestCachedHandlerForEmptyGatherErrorIsNotCached(t *testing.T) {
	var gathers atomic.Int64
	h := CachedHandlerFor(prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { gathers.Inc(); return nil, errors.New("gather failed") }), promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError}, time.Minute)
	for _, path := range []string{"/metrics", "/metrics?name[]=missing", "/metrics"} {
		require.Equal(t, http.StatusInternalServerError, scrapeMetricsPath(h, path, "", "").Code)
	}
	require.Equal(t, int64(3), gathers.Load())
}
