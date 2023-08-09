// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/http_tracing.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/uber/jaeger-client-go"
)

// Dummy dependency to enforce that we have a nethttp version newer
// than the one which implements Websockets. (No semver on nethttp)
var _ = nethttp.MWURLTagFunc

// Tracer is a middleware which traces incoming requests.
type Tracer struct {
	RouteMatcher RouteMatcher
	SourceIPs    *SourceIPExtractor
}

// Wrap implements Interface
func (t Tracer) Wrap(next http.Handler) http.Handler {
	options := []nethttp.MWOption{
		nethttp.OperationNameFunc(makeHTTPOperationNameFunc(t.RouteMatcher)),
		nethttp.MWSpanObserver(func(sp opentracing.Span, r *http.Request) {
			// add a tag with the client's user agent to the span
			userAgent := r.Header.Get("User-Agent")
			if userAgent != "" {
				sp.SetTag("http.user_agent", userAgent)
			}

			// add a tag with the client's sourceIPs to the span, if a
			// SourceIPExtractor is given.
			if t.SourceIPs != nil {
				sp.SetTag("sourceIPs", t.SourceIPs.Get(r))
			}
		}),
	}

	return nethttp.Middleware(opentracing.GlobalTracer(), next, options...)
}

const httpGRPCHandleMethod = "/httpgrpc.HTTP/Handle"

// HTTPGRPCTracer is a middleware which traces incoming requests.
type HTTPGRPCTracer struct {
	RouteMatcher RouteMatcher
}

// InitHTTPGRPCMiddleware initializes gorilla/mux-compatible HTTP middleware
func InitHTTPGRPCMiddleware(router *mux.Router) {
	middleware := HTTPGRPCTracer{RouteMatcher: router}
	router.Use(middleware.Wrap)
}

// Wrap implements Interface
func (hgt HTTPGRPCTracer) Wrap(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tracer := opentracing.GlobalTracer()

		// skip spans which were not forwarded from non-httpgrpc.HTTP/Handle spans;
		// standard http spans started directly from the HTTP server will already be instrumented
		parentSpan := opentracing.SpanFromContext(ctx)
		if parentSpan, ok := parentSpan.(*jaeger.Span); ok {
			if parentSpan.OperationName() != httpGRPCHandleMethod {
				next.ServeHTTP(w, r)
				return
			}
		}

		// extract relevant span & tag data from request
		method := r.Method
		matchedRoute := getRouteName(hgt.RouteMatcher, r)
		url := r.URL.String()
		userAgent := r.Header.Get("User-Agent")

		// tag parent httpgrpc.HTTP/Handle server span
		parentSpan.SetTag(string(ext.HTTPUrl), url)
		parentSpan.SetTag(string(ext.HTTPMethod), method)
		parentSpan.SetTag("http.route", matchedRoute)
		parentSpan.SetTag("http.user_agent", userAgent)

		// create and start child HTTP span
		// mirroring opentracing-contrib/go-stdlib/nethttp approach
		spanCtx, _ := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
		childSpanName := makeHTTPOperationNameFunc(hgt.RouteMatcher)(r)
		startSpanOpts := []opentracing.StartSpanOption{
			ext.RPCServerOption(spanCtx),
			opentracing.Tag{Key: string(ext.Component), Value: "net/http"},
			opentracing.Tag{Key: string(ext.HTTPUrl), Value: url},
			opentracing.Tag{Key: string(ext.HTTPMethod), Value: method},
			opentracing.Tag{Key: "http.route", Value: matchedRoute},
			opentracing.Tag{Key: "http.user_agent", Value: userAgent},
		}
		childSpan := tracer.StartSpan(childSpanName, startSpanOpts...)
		defer childSpan.Finish()

		r = r.WithContext(opentracing.ContextWithSpan(r.Context(), childSpan))
		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

func makeHTTPOperationNameFunc(routeMatcher RouteMatcher) func(r *http.Request) string {
	return func(r *http.Request) string {
		op := getRouteName(routeMatcher, r)
		if op == "" {
			return "HTTP " + r.Method
		}
		return fmt.Sprintf("HTTP %s - %s", r.Method, op)
	}
}
