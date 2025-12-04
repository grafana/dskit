// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package promhttpfork is a fork of github.com/prometheus/client_golang/prometheus/promhttp
// with the PR https://github.com/prometheus/client_golang/pull/1925 applied.
// It is expected to disappear once that PR is merged and released, and we can switch back to the upstream package.
package promhttpfork

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"

	"github.com/grafana/dskit/server/promhttpfork/internal/github.com/golang/gddo/httputil"
)

const (
	contentTypeHeader      = "Content-Type"
	contentEncodingHeader  = "Content-Encoding"
	acceptEncodingHeader   = "Accept-Encoding"
	processStartTimeHeader = "Process-Start-Time-Unix"
)

func defaultCompressionFormats() []promhttp.Compression {
	return []promhttp.Compression{promhttp.Identity, promhttp.Gzip}
}

var gzipPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}

// Handler returns an http.Handler for the prometheus.DefaultGatherer, using
// default HandlerOpts, i.e. it reports the first error as an HTTP error, it has
// no error logging, and it applies compression if requested by the client.
//
// The returned http.Handler is already instrumented using the
// InstrumentMetricHandler function and the prometheus.DefaultRegisterer. If you
// create multiple http.Handlers by separate calls of the Handler function, the
// metrics used for instrumentation will be shared between them, providing
// global scrape counts.
//
// This function is meant to cover the bulk of basic use cases. If you are doing
// anything that requires more customization (including using a non-default
// Gatherer, different instrumentation, and non-default HandlerOpts), use the
// HandlerFor function. See there for details.
func Handler() http.Handler {
	return promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer, HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}),
	)
}

// HandlerFor returns an uninstrumented http.Handler for the provided
// Gatherer. The behavior of the Handler is defined by the provided
// HandlerOpts. Thus, HandlerFor is useful to create http.Handlers for custom
// Gatherers, with non-default HandlerOpts, and/or with custom (or no)
// instrumentation. Use the InstrumentMetricHandler function to apply the same
// kind of instrumentation as it is used by the Handler function.
func HandlerFor(reg prometheus.Gatherer, opts promhttp.HandlerOpts) http.Handler {
	return HandlerForTransactional(prometheus.ToTransactionalGatherer(reg), opts)
}

// HandlerForTransactional is like HandlerFor, but it uses transactional gather, which
// can safely change in-place returned *dto.MetricFamily before call to `Gather` and after
// call to `done` of that `Gather`.
func HandlerForTransactional(reg prometheus.TransactionalGatherer, opts promhttp.HandlerOpts) http.Handler {
	var (
		inFlightSem chan struct{}
		errCnt      = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "promhttp_metric_handler_errors_total",
				Help: "Total number of internal errors encountered by the promhttp metric handler.",
			},
			[]string{"cause"},
		)
	)

	if opts.MaxRequestsInFlight > 0 {
		inFlightSem = make(chan struct{}, opts.MaxRequestsInFlight)
	}
	if opts.Registry != nil {
		// Initialize all possibilities that can occur below.
		errCnt.WithLabelValues("gathering")
		errCnt.WithLabelValues("encoding")
		if err := opts.Registry.Register(errCnt); err != nil {
			are := &prometheus.AlreadyRegisteredError{}
			if errors.As(err, are) {
				errCnt = are.ExistingCollector.(*prometheus.CounterVec)
			} else {
				panic(err)
			}
		}
	}

	// Select compression formats to offer based on default or user choice.
	var compressions []string
	if !opts.DisableCompression {
		offers := defaultCompressionFormats()
		if len(opts.OfferedCompressions) > 0 {
			offers = opts.OfferedCompressions
		}
		for _, comp := range offers {
			compressions = append(compressions, string(comp))
		}
	}

	h := http.HandlerFunc(func(rsp http.ResponseWriter, req *http.Request) {
		if !opts.ProcessStartTime.IsZero() {
			rsp.Header().Set(processStartTimeHeader, strconv.FormatInt(opts.ProcessStartTime.Unix(), 10))
		}
		if inFlightSem != nil {
			select {
			case inFlightSem <- struct{}{}: // All good, carry on.
				defer func() { <-inFlightSem }()
			default:
				http.Error(rsp, fmt.Sprintf(
					"Limit of concurrent requests reached (%d), try again later.", opts.MaxRequestsInFlight,
				), http.StatusServiceUnavailable)
				return
			}
		}
		mfs, done, err := reg.Gather()
		defer done()
		if err != nil {
			if opts.ErrorLog != nil {
				opts.ErrorLog.Println("error gathering metrics:", err)
			}
			errCnt.WithLabelValues("gathering").Inc()
			switch opts.ErrorHandling {
			case promhttp.PanicOnError:
				panic(err)
			case promhttp.ContinueOnError:
				if len(mfs) == 0 {
					// Still report the error if no metrics have been gathered.
					httpError(rsp, err)
					return
				}
			case promhttp.HTTPErrorOnError:
				httpError(rsp, err)
				return
			}
		}

		var contentType expfmt.Format
		if opts.EnableOpenMetrics {
			contentType = expfmt.NegotiateIncludingOpenMetrics(req.Header)
		} else {
			contentType = expfmt.Negotiate(req.Header)
		}
		rsp.Header().Set(contentTypeHeader, string(contentType))

		w, encodingHeader, closeWriter, err := negotiateEncodingWriter(req, rsp, compressions)
		if err != nil {
			if opts.ErrorLog != nil {
				opts.ErrorLog.Println("error getting writer", err)
			}
			w = io.Writer(rsp)
			encodingHeader = string(promhttp.Identity)
		}

		defer closeWriter()

		// Set Content-Encoding only when data is compressed
		if encodingHeader != string(promhttp.Identity) {
			rsp.Header().Set(contentEncodingHeader, encodingHeader)
		}

		var enc expfmt.Encoder
		if opts.EnableOpenMetricsTextCreatedSamples {
			enc = expfmt.NewEncoder(w, contentType, expfmt.WithCreatedLines())
		} else {
			enc = expfmt.NewEncoder(w, contentType)
		}

		// handleError handles the error according to opts.ErrorHandling
		// and returns true if we have to abort after the handling.
		handleError := func(err error) bool {
			if err == nil {
				return false
			}
			if opts.ErrorLog != nil {
				opts.ErrorLog.Println("error encoding and sending metric family:", err)
			}
			errCnt.WithLabelValues("encoding").Inc()
			switch opts.ErrorHandling {
			case promhttp.PanicOnError:
				panic(err)
			case promhttp.HTTPErrorOnError:
				// We cannot really send an HTTP error at this
				// point because we most likely have written
				// something to rsp already. But at least we can
				// stop sending.
				return true
			}
			// Do nothing in all other cases, including ContinueOnError.
			return false
		}

		// Build metric name filter set from query params (if any)
		var metricFilter map[string]struct{}
		if metricNames := req.URL.Query()["name[]"]; len(metricNames) > 0 {
			metricFilter = make(map[string]struct{}, len(metricNames))
			for _, name := range metricNames {
				metricFilter[name] = struct{}{}
			}
		}

		for _, mf := range mfs {
			if metricFilter != nil {
				if _, ok := metricFilter[mf.GetName()]; !ok {
					continue
				}
			}
			if handleError(enc.Encode(mf)) {
				return
			}
		}
		if closer, ok := enc.(expfmt.Closer); ok {
			// This in particular takes care of the final "# EOF\n" line for OpenMetrics.
			if handleError(closer.Close()) {
				return
			}
		}
	})

	if opts.Timeout <= 0 {
		return h
	}
	return http.TimeoutHandler(h, opts.Timeout, fmt.Sprintf(
		"Exceeded configured timeout of %v.\n",
		opts.Timeout,
	))
}

// InstrumentMetricHandler is usually used with an http.Handler returned by the
// HandlerFor function. It instruments the provided http.Handler with two
// metrics: A counter vector "promhttp_metric_handler_requests_total" to count
// scrapes partitioned by HTTP status code, and a gauge
// "promhttp_metric_handler_requests_in_flight" to track the number of
// simultaneous scrapes. This function idempotently registers collectors for
// both metrics with the provided Registerer. It panics if the registration
// fails. The provided metrics are useful to see how many scrapes hit the
// monitored target (which could be from different Prometheus servers or other
// scrapers), and how often they overlap (which would result in more than one
// scrape in flight at the same time). Note that the scrapes-in-flight gauge
// will contain the scrape by which it is exposed, while the scrape counter will
// only get incremented after the scrape is complete (as only then the status
// code is known). For tracking scrape durations, use the
// "scrape_duration_seconds" gauge created by the Prometheus server upon each
// scrape.
func InstrumentMetricHandler(reg prometheus.Registerer, handler http.Handler) http.Handler {
	cnt := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "promhttp_metric_handler_requests_total",
			Help: "Total number of scrapes by HTTP status code.",
		},
		[]string{"code"},
	)
	// Initialize the most likely HTTP status codes.
	cnt.WithLabelValues("200")
	cnt.WithLabelValues("500")
	cnt.WithLabelValues("503")
	if err := reg.Register(cnt); err != nil {
		are := &prometheus.AlreadyRegisteredError{}
		if errors.As(err, are) {
			cnt = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}

	gge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "promhttp_metric_handler_requests_in_flight",
		Help: "Current number of scrapes being served.",
	})
	if err := reg.Register(gge); err != nil {
		are := &prometheus.AlreadyRegisteredError{}
		if errors.As(err, are) {
			gge = are.ExistingCollector.(prometheus.Gauge)
		} else {
			panic(err)
		}
	}

	return promhttp.InstrumentHandlerCounter(cnt, promhttp.InstrumentHandlerInFlight(gge, handler))
}

// Logger is the minimal interface HandlerOpts needs for logging. Note that
// log.Logger from the standard library implements this interface, and it is
// easy to implement by custom loggers, if they don't do so already anyway.
type Logger interface {
	Println(v ...interface{})
}

// httpError removes any content-encoding header and then calls http.Error with
// the provided error and http.StatusInternalServerError. Error contents is
// supposed to be uncompressed plain text. Same as with a plain http.Error, this
// must not be called if the header or any payload has already been sent.
func httpError(rsp http.ResponseWriter, err error) {
	rsp.Header().Del(contentEncodingHeader)
	http.Error(
		rsp,
		"An error has occurred while serving metrics:\n\n"+err.Error(),
		http.StatusInternalServerError,
	)
}

// negotiateEncodingWriter reads the Accept-Encoding header from a request and
// selects the right compression based on an allow-list of supported
// compressions. It returns a writer implementing the compression and an the
// correct value that the caller can set in the response header.
func negotiateEncodingWriter(r *http.Request, rw io.Writer, compressions []string) (_ io.Writer, encodingHeaderValue string, closeWriter func(), _ error) {
	if len(compressions) == 0 {
		return rw, string(promhttp.Identity), func() {}, nil
	}

	// TODO(mrueg): Replace internal/github.com/gddo once https://github.com/golang/go/issues/19307 is implemented.
	selected := httputil.NegotiateContentEncoding(r, compressions)

	switch selected {
	case "gzip":
		gz := gzipPool.Get().(*gzip.Writer)
		gz.Reset(rw)
		return gz, selected, func() { _ = gz.Close(); gzipPool.Put(gz) }, nil
	case "identity":
		// This means the content is not compressed.
		return rw, selected, func() {}, nil
	default:
		// The content encoding was not implemented yet.
		return nil, "", func() {}, fmt.Errorf("content compression format not recognized: %s. Valid formats are: %s", selected, defaultCompressionFormats())
	}
}
