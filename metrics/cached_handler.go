package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/atomic"
	"golang.org/x/sync/singleflight"
)

const metricsResponseCacheSize = 8

// CachedHandlerFor is promhttp.HandlerFor with an in-memory response cache.
// A non-positive cacheDuration disables caching. Successful GET responses are
// cached for at most cacheDuration from the start of collection, independently
// for up to eight Accept/Accept-Encoding combinations. Full and name[]-filtered
// scrapes share one collection snapshot and encoded metric families. Other query
// parameters bypass the cache. Cached metrics can lag behind live values by
// cacheDuration; choose a duration appropriate for scrape intervals and alerts.
// With caching enabled, overlapping collections are always coalesced across
// variants and bypassed requests, regardless of opts.CoalesceGather.
func CachedHandlerFor(reg prometheus.Gatherer, opts promhttp.HandlerOpts, cacheDuration time.Duration) http.Handler {
	if cacheDuration <= 0 {
		return promhttp.HandlerFor(reg, opts)
	}

	timeout := opts.Timeout
	// Cache hits are written directly and refreshes run detached from requests, so
	// the timeout applies to waiting for a refresh and streaming filtered responses.
	opts.Timeout = 0
	var inFlight chan struct{}
	if opts.MaxRequestsInFlight > 0 {
		inFlight = make(chan struct{}, opts.MaxRequestsInFlight)
		opts.MaxRequestsInFlight = 0
	}
	reg = &metricsCoalescingGatherer{reg: reg}
	opts.CoalesceGather = false
	responses, err := lru.New[metricsResponseCacheKey, *metricsResponse](metricsResponseCacheSize)
	if err != nil {
		panic(err)
	}
	// Refreshes need separate error logs, but concurrency controls must span
	// every representation and the uncached path.
	bypass := limitMetricsRequests(promhttp.HandlerFor(reg, opts), inFlight)
	// Reuse promhttp's registered counter for errors while streaming cached families.
	var encodingErrors prometheus.Counter
	if opts.Registry != nil {
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{ //nolint:forbidigo // Must match the collector registered by promhttp.HandlerFor above.
			Name: "promhttp_metric_handler_errors_total",
			Help: "Total number of internal errors encountered by the promhttp metric handler.",
		}, []string{"cause"})
		if err := opts.Registry.Register(counter); err != nil {
			var registered prometheus.AlreadyRegisteredError
			if !errors.As(err, &registered) {
				panic(err)
			}
			counter = registered.ExistingCollector.(*prometheus.CounterVec)
		}
		encodingErrors = counter.WithLabelValues("encoding")
	}
	if timeout > 0 {
		bypass = http.TimeoutHandler(bypass, timeout, metricsTimeoutBody(timeout))
	}
	return &cachedMetricsHandler{
		reg:            reg,
		encodingErrors: encodingErrors,
		opts:           opts,
		inFlight:       inFlight,
		bypass:         bypass,
		duration:       cacheDuration,
		timeout:        timeout,
		now:            time.Now,
		responses:      responses,
		refreshes:      map[metricsResponseCacheKey]*metricsRefresh{},
	}
}

// metricsTimeoutBody matches the body promhttp writes when its own timeout expires.
func metricsTimeoutBody(timeout time.Duration) string {
	return fmt.Sprintf("Exceeded configured timeout of %v.\n", timeout)
}

type metricsResponseCacheKey struct{ accept, encoding string }

type metricsResponse struct {
	header       http.Header
	body         []byte
	expires      time.Time
	snapshot     *metricsSnapshot
	encoded      *metricsFamilyEncoding
	fallbackOnce sync.Once
	fallback     metricsRefresh
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

// metricsRefresh is a collection shared by every request for the same key that
// arrives while it runs.
type metricsRefresh struct {
	done     chan struct{}
	response *metricsResponse
	code     int
	panicked any
}

type cachedMetricsHandler struct {
	reg            prometheus.Gatherer
	encodingErrors prometheus.Counter
	opts           promhttp.HandlerOpts
	inFlight       chan struct{}
	bypass         http.Handler
	duration       time.Duration
	timeout        time.Duration
	now            func() time.Time

	mu        sync.Mutex
	responses *lru.Cache[metricsResponseCacheKey, *metricsResponse]
	refreshes map[metricsResponseCacheKey]*metricsRefresh
	snapshot  *metricsSnapshot
	gathers   singleflight.Group
}

func (h *cachedMetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var names []string
	if r.Method != http.MethodGet {
		h.bypass.ServeHTTP(w, r)
		return
	}
	if r.URL != nil && r.URL.RawQuery != "" {
		params, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil || len(params) != 1 || len(params["name[]"]) == 0 {
			h.bypass.ServeHTTP(w, r)
			return
		}
		names = params["name[]"]
	}
	key := metricsResponseCacheKey{accept: r.Header.Get("Accept"), encoding: strings.Join(r.Header.Values("Accept-Encoding"), "\x00")}
	h.mu.Lock()
	if response, ok := h.responses.Get(key); ok && h.now().Before(response.expires) {
		h.mu.Unlock()
		h.writeResponse(w, r, response, http.StatusOK, names)
		return
	}
	refresh, ok := h.refreshes[key]
	if !ok {
		refresh = &metricsRefresh{done: make(chan struct{})}
		h.refreshes[key] = refresh
		// The refresh outlives a canceled or timed-out first request so that the
		// other waiters still get its result.
		go h.refresh(r.Clone(context.WithoutCancel(r.Context())), key, refresh)
	}
	h.mu.Unlock()

	var timedOut <-chan time.Time
	if h.timeout > 0 {
		timer := time.NewTimer(h.timeout)
		defer timer.Stop()
		timedOut = timer.C
	}
	select {
	case <-refresh.done:
	case <-r.Context().Done():
		return
	case <-timedOut:
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, metricsTimeoutBody(h.timeout))
		return
	}
	if refresh.panicked != nil {
		panic(refresh.panicked)
	}
	h.writeResponse(w, r, refresh.response, refresh.code, names)
}

func (h *cachedMetricsHandler) refresh(r *http.Request, key metricsResponseCacheKey, refresh *metricsRefresh) {
	cacheable := false
	defer func() {
		// Re-raised in waiters so PanicOnError behaves as it does without the cache.
		refresh.panicked = recover()
		h.mu.Lock()
		if cacheable {
			h.responses.Add(key, refresh.response)
		}
		delete(h.refreshes, key)
		h.mu.Unlock()
		close(refresh.done)
	}()

	// Every filter set uses the full representation's refresh, without retaining
	// cache entries or compressed payloads for arbitrary combinations of names.
	if r.URL != nil {
		u := *r.URL
		u.RawQuery = ""
		r.URL = &u
	}
	writer := &metricsResponseWriter{header: make(http.Header)}
	refresh.response = &metricsResponse{}
	limitMetricsRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.renderSnapshot(writer, r, refresh.response)
	}), h.inFlight).ServeHTTP(writer, r)
	if writer.code == 0 {
		writer.code = http.StatusOK
	}
	refresh.code = writer.code
	refresh.response.header = writer.header
	if refresh.response.encoded != nil && writer.header.Get("Content-Encoding") == "" {
		// Identity responses can share the immutable family buffer directly.
		refresh.response.body = refresh.response.encoded.body
	} else {
		refresh.response.body = bytes.Clone(writer.body.Bytes())
	}
	if snapshot := refresh.response.snapshot; snapshot != nil {
		refresh.response.expires = snapshot.expires
	}
	cacheable = writer.code == http.StatusOK &&
		(refresh.response.encoded != nil || h.opts.ErrorHandling == promhttp.ContinueOnError && refresh.response.snapshot != nil && len(refresh.response.snapshot.families) > 0) &&
		h.now().Before(refresh.response.expires)
}

func limitMetricsRequests(next http.Handler, inFlight chan struct{}) http.Handler {
	if inFlight == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case inFlight <- struct{}{}:
			defer func() { <-inFlight }()
			next.ServeHTTP(w, r)
		default:
			http.Error(w, fmt.Sprintf("Limit of concurrent requests reached (%d), try again later.", cap(inFlight)), http.StatusServiceUnavailable)
		}
	})
}

type metricsCoalescingGatherer struct {
	reg   prometheus.Gatherer
	group singleflight.Group
}

type metricsGatherResult struct {
	families []*dto.MetricFamily
	err      error
	panicked any
}

func (g *metricsCoalescingGatherer) Gather() ([]*dto.MetricFamily, error) {
	v, _, _ := g.group.Do("", func() (any, error) {
		result := &metricsGatherResult{}
		func() {
			// Keep the original panic value so each handler applies normal recovery.
			defer func() { result.panicked = recover() }()
			result.families, result.err = g.reg.Gather()
		}()
		return result, nil
	})
	result := v.(*metricsGatherResult)
	if result.panicked != nil {
		panic(result.panicked)
	}
	return slices.Clone(result.families), result.err
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
