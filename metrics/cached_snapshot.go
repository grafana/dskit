package metrics

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
)

// A snapshot owns its protobufs so collectors cannot mutate data still being served.
type metricsSnapshot struct {
	families []*dto.MetricFamily
	err      error
	expires  time.Time
	formats  *lru.Cache[expfmt.Format, *metricsFamilyEncoding]
	encodes  singleflight.Group
}

type metricsSnapshotResult struct {
	snapshot *metricsSnapshot
	panicked any
}

func (h *cachedMetricsHandler) snapshotFor() *metricsSnapshot {
	h.mu.Lock()
	if snapshot := h.snapshot; snapshot != nil && h.now().Before(snapshot.expires) {
		h.mu.Unlock()
		return snapshot
	}
	h.mu.Unlock()

	v, _, _ := h.gathers.Do("", func() (any, error) {
		result := &metricsSnapshotResult{}
		func() {
			defer func() { result.panicked = recover() }()
			h.mu.Lock()
			if snapshot := h.snapshot; snapshot != nil && h.now().Before(snapshot.expires) {
				result.snapshot = snapshot
				h.mu.Unlock()
				return
			}
			h.mu.Unlock()

			start := h.now()
			families, err := h.reg.Gather()
			snapshot := &metricsSnapshot{err: err, expires: start.Add(h.duration)}
			snapshot.formats, _ = lru.New[expfmt.Format, *metricsFamilyEncoding](metricsResponseCacheSize)
			for _, family := range families {
				snapshot.families = append(snapshot.families, proto.Clone(family).(*dto.MetricFamily))
			}
			result.snapshot = snapshot
			if (err == nil || h.opts.ErrorHandling == promhttp.ContinueOnError && len(families) > 0) && h.now().Before(snapshot.expires) {
				h.mu.Lock()
				h.snapshot = snapshot
				// Expired representations must not retain previous protobuf generations.
				h.responses.Purge()
				h.mu.Unlock()
			}
		}()
		return result, nil
	})
	result := v.(*metricsSnapshotResult)
	if result.panicked != nil {
		panic(result.panicked)
	}
	return result.snapshot
}

func (h *cachedMetricsHandler) discardSnapshot(snapshot *metricsSnapshot) {
	h.mu.Lock()
	if h.snapshot == snapshot {
		h.snapshot = nil
	}
	h.mu.Unlock()
}

func (s *metricsSnapshot) Gather() ([]*dto.MetricFamily, error) {
	return slices.Clone(s.families), s.err
}

type metricsFamilyRange struct{ start, end int }

type metricsFamilyEncoding struct {
	body     []byte
	families []metricsFamilyRange
	byName   map[string]int
	footer   int
}

func (s *metricsSnapshot) encoding(format expfmt.Format, created bool) (*metricsFamilyEncoding, error) {
	if encoded, ok := s.formats.Get(format); ok {
		return encoded, nil
	}
	v, err, _ := s.encodes.Do(string(format), func() (any, error) {
		if encoded, ok := s.formats.Get(format); ok {
			return encoded, nil
		}
		var body bytes.Buffer
		var opts []expfmt.EncoderOption
		if created {
			opts = append(opts, expfmt.WithCreatedLines())
		}
		encoder := expfmt.NewEncoder(&body, format, opts...)
		encoded := &metricsFamilyEncoding{byName: make(map[string]int, len(s.families))}
		for _, family := range s.families {
			start := body.Len()
			if err := encoder.Encode(family); err != nil {
				return nil, err
			}
			encoded.byName[family.GetName()] = len(encoded.families)
			encoded.families = append(encoded.families, metricsFamilyRange{start: start, end: body.Len()})
		}
		encoded.footer = body.Len()
		if closer, ok := encoder.(expfmt.Closer); ok {
			if err := closer.Close(); err != nil {
				return nil, err
			}
		}
		encoded.body = bytes.Clone(body.Bytes())
		s.formats.Add(format, encoded)
		return encoded, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*metricsFamilyEncoding), nil
}

func (e *metricsFamilyEncoding) writeTo(w io.Writer, names []string) error {
	if names == nil {
		_, err := w.Write(e.body)
		return err
	}
	indices := make([]int, 0, len(names))
	for _, name := range names {
		if index, ok := e.byName[name]; ok {
			indices = append(indices, index)
		}
	}
	sort.Ints(indices)
	for _, index := range slices.Compact(indices) {
		family := e.families[index]
		if _, err := w.Write(e.body[family.start:family.end]); err != nil {
			return err
		}
	}
	// OpenMetrics has one stream footer, including for an empty selection.
	_, err := w.Write(e.body[e.footer:])
	return err
}

// Negotiation stays with promhttp, including optional compression support and Vary.
func (h *cachedMetricsHandler) negotiate(r *http.Request) (http.Header, bool) {
	log := &metricsHandlerErrorLog{}
	opts := h.opts
	opts.ErrorLog = log
	opts.Registry = nil
	writer := &metricsResponseWriter{header: make(http.Header)}
	promhttp.HandlerFor(prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return nil, nil }), opts).ServeHTTP(writer, r)
	return writer.header, log.errors.Load() == 0
}

var metricsGzipPool = sync.Pool{New: func() any { return gzip.NewWriter(nil) }}

func writeMetricsEncoding(w io.Writer, encoding string, families *metricsFamilyEncoding, names []string) error {
	switch encoding {
	case "":
		return families.writeTo(w, names)
	case "gzip":
		gz := metricsGzipPool.Get().(*gzip.Writer)
		gz.Reset(w)
		err := families.writeTo(gz, names)
		closeErr := gz.Close()
		metricsGzipPool.Put(gz)
		if err != nil {
			return err
		}
		return closeErr
	case "zstd":
		z, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			return err
		}
		err = families.writeTo(z, names)
		closeErr := z.Close()
		if err != nil {
			return err
		}
		return closeErr
	default:
		return fmt.Errorf("unsupported metrics content encoding %q", encoding)
	}
}

func (h *cachedMetricsHandler) renderSnapshot(writer *metricsResponseWriter, r *http.Request, response *metricsResponse) {
	snapshot := h.snapshotFor()
	response.snapshot = snapshot
	if snapshot.err == nil {
		if header, ok := h.negotiate(r); ok {
			encoded, err := snapshot.encoding(expfmt.Format(header.Get("Content-Type")), h.opts.EnableOpenMetricsTextCreatedSamples)
			if err == nil {
				var body bytes.Buffer
				if compression := header.Get("Content-Encoding"); compression != "" {
					if err := writeMetricsEncoding(&body, compression, encoded, nil); err != nil {
						return
					}
				}
				response.encoded = encoded
				writer.header = header
				writer.body = body
				return
			}
		}
	}
	// A filtered caller may not select the family that failed to encode. Defer
	// promhttp error handling until the actual request's filter is known.
	if h.opts.ErrorHandling != promhttp.ContinueOnError {
		h.discardSnapshot(snapshot)
	}
}

func (h *cachedMetricsHandler) writeResponse(w http.ResponseWriter, r *http.Request, response *metricsResponse, code int, names []string) {
	if names == nil && response.encoded != nil || response.snapshot == nil {
		response.writeTo(w, code)
		return
	}
	filtered := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if response.encoded == nil {
			if names != nil {
				promhttp.HandlerFor(response.snapshot, h.opts).ServeHTTP(w, r)
				return
			}
			response.fallbackOnce.Do(func() {
				defer func() { response.fallback.panicked = recover() }()
				writer := &metricsResponseWriter{header: make(http.Header)}
				promhttp.HandlerFor(response.snapshot, h.opts).ServeHTTP(writer, r)
				code := writer.code
				if code == 0 {
					code = http.StatusOK
				}
				response.fallback.code = code
				response.fallback.response = &metricsResponse{header: writer.header, body: bytes.Clone(writer.body.Bytes())}
			})
			if response.fallback.panicked != nil {
				panic(response.fallback.panicked)
			}
			response.fallback.response.writeTo(w, response.fallback.code)
			return
		}
		for key, values := range response.header {
			if key == "Vary" {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			} else {
				w.Header()[key] = slices.Clone(values)
			}
		}
		if err := writeMetricsEncoding(w, response.header.Get("Content-Encoding"), response.encoded, names); err != nil {
			if h.opts.ErrorLog != nil {
				h.opts.ErrorLog.Println("error encoding and sending metric family:", err)
			}
			if h.encodingErrors != nil {
				h.encodingErrors.Inc()
			}
			if h.opts.ErrorHandling == promhttp.PanicOnError {
				panic(err)
			}
		}
	})
	handler := limitMetricsRequests(filtered, h.inFlight)
	if h.timeout > 0 {
		handler = http.TimeoutHandler(handler, h.timeout, metricsTimeoutBody(h.timeout))
	}
	handler.ServeHTTP(w, r)
}
