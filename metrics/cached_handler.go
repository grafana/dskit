package metrics

import (
	"bytes"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/atomic"
)

const metricsResponseCacheSize = 8

// CachedHandlerFor is promhttp.HandlerFor with an in-memory response cache.
// A non-positive cacheDuration disables caching. Successful GET responses are
// cached for at most cacheDuration from the start of collection, independently
// for up to eight Accept/Accept-Encoding combinations. Requests with query
// parameters bypass the cache. Cached metrics can lag behind live values by
// cacheDuration; choose a duration appropriate for scrape intervals and alerts.
func CachedHandlerFor(reg prometheus.Gatherer, opts promhttp.HandlerOpts, cacheDuration time.Duration) http.Handler {
	if cacheDuration <= 0 {
		return promhttp.HandlerFor(reg, opts)
	}

	// promhttp can return a partial body with status 200 on collection or encoding
	// errors. Track its error reporting so those responses are never cached.
	errorLog := &metricsHandlerErrorLog{next: opts.ErrorLog}
	opts.ErrorLog = errorLog
	timeout := opts.Timeout
	opts.Timeout = 0
	responses, err := lru.New[metricsResponseCacheKey, *metricsResponse](metricsResponseCacheSize)
	if err != nil {
		panic(err)
	}
	h := &cachedMetricsHandler{
		next:       promhttp.HandlerFor(reg, opts),
		duration:   cacheDuration,
		now:        time.Now,
		responses:  responses,
		refreshing: make(chan struct{}, 1),
		errorLog:   errorLog,
	}
	if timeout > 0 {
		// The timeout must also cover waiting for an in-flight refresh.
		return http.TimeoutHandler(h, timeout, fmt.Sprintf("Exceeded configured timeout of %v.\n", timeout))
	}
	return h
}

type metricsResponseCacheKey struct{ accept, encoding string }

type metricsResponse struct {
	header  http.Header
	body    []byte
	expires time.Time
}

func (r *metricsResponse) writeTo(w http.ResponseWriter, code int) {
	for key, values := range r.header {
		// Preserve negotiation dimensions already added by outer middleware.
		if key == "Vary" {
			for _, value := range values {
				w.Header().Add(key, value)
			}
			continue
		}
		w.Header()[key] = slices.Clone(values)
	}
	w.WriteHeader(code)
	_, _ = w.Write(r.body)
}

type cachedMetricsHandler struct {
	next       http.Handler
	duration   time.Duration
	now        func() time.Time
	responses  *lru.Cache[metricsResponseCacheKey, *metricsResponse]
	refreshing chan struct{}
	errorLog   *metricsHandlerErrorLog
}

func (h *cachedMetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Filtered scrapes must not populate the full-registry snapshot, and arbitrary
	// query strings must not create an unbounded number of cache entries.
	if r.Method != http.MethodGet || (r.URL != nil && r.URL.RawQuery != "") {
		h.next.ServeHTTP(w, r)
		return
	}
	key := metricsResponseCacheKey{accept: r.Header.Get("Accept"), encoding: strings.Join(r.Header.Values("Accept-Encoding"), "\x00")}
	if response, ok := h.cached(key); ok {
		response.writeTo(w, http.StatusOK)
		return
	}
	response, code := h.refresh(r, key)
	if response == nil {
		return
	}
	response.writeTo(w, code)
}

func (h *cachedMetricsHandler) cached(key metricsResponseCacheKey) (*metricsResponse, bool) {
	response, ok := h.responses.Get(key)
	return response, ok && h.now().Before(response.expires)
}

func (h *cachedMetricsHandler) refresh(r *http.Request, key metricsResponseCacheKey) (*metricsResponse, int) {
	// Serialize refreshes without holding a lock during writes to slow clients.
	// Waiters can abandon a refresh when their scrape is canceled or times out.
	select {
	case h.refreshing <- struct{}{}:
		defer func() { <-h.refreshing }()
	case <-r.Context().Done():
		return nil, 0
	}
	if r.Context().Err() != nil {
		return nil, 0
	}
	if response, ok := h.cached(key); ok {
		return response, http.StatusOK
	}

	start := h.now()
	errorsBefore := h.errorLog.errors.Load()
	writer := &metricsResponseWriter{header: make(http.Header)}
	h.next.ServeHTTP(writer, r)
	if writer.code == 0 {
		writer.code = http.StatusOK
	}
	response := &metricsResponse{header: writer.header, body: writer.body.Bytes(), expires: start.Add(h.duration)}
	if writer.code == http.StatusOK && h.errorLog.errors.Load() == errorsBefore && r.Context().Err() == nil && h.now().Before(response.expires) {
		// Buffer growth can leave nearly twice the payload size retained per variant.
		response.body = bytes.Clone(response.body)
		h.responses.Add(key, response)
	}
	return response, writer.code
}

type metricsResponseWriter struct {
	header http.Header
	body   bytes.Buffer
	code   int
}

func (w *metricsResponseWriter) Header() http.Header { return w.header }
func (w *metricsResponseWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
}
func (w *metricsResponseWriter) Write(p []byte) (int, error) {
	if w.code == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.body.Write(p)
}

type metricsHandlerErrorLog struct {
	next   promhttp.Logger
	errors atomic.Uint64
}

func (l *metricsHandlerErrorLog) Println(v ...any) {
	l.errors.Inc()
	if l.next != nil {
		l.next.Println(v...)
	}
}
