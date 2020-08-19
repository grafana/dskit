package middleware

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/felixge/httpsnoop"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
)

// RouteMatcher matches routes
type RouteMatcher interface {
	Match(*http.Request, *mux.RouteMatch) bool
}

// Instrument is a Middleware which records timings for every HTTP request
type Instrument struct {
	RouteMatcher RouteMatcher
	Duration     *prometheus.HistogramVec
}

// IsWSHandshakeRequest returns true if the given request is a websocket handshake request.
func IsWSHandshakeRequest(req *http.Request) bool {
	if strings.ToLower(req.Header.Get("Upgrade")) == "websocket" {
		// Connection header values can be of form "foo, bar, ..."
		parts := strings.Split(strings.ToLower(req.Header.Get("Connection")), ",")
		for _, part := range parts {
			if strings.TrimSpace(part) == "upgrade" {
				return true
			}
		}
	}
	return false
}

// Wrap implements middleware.Interface
func (i Instrument) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isWS := strconv.FormatBool(IsWSHandshakeRequest(r))
		respMetrics := httpsnoop.CaptureMetricsFn(w, func(ww http.ResponseWriter) {
			next.ServeHTTP(ww, r)
		})

		route := i.getRouteName(r)
		i.Duration.WithLabelValues(r.Method, route, strconv.Itoa(respMetrics.Code), isWS).Observe(respMetrics.Duration.Seconds())
	})
}

// Return a name identifier for ths request.  There are three options:
//   1. The request matches a gorilla mux route, with a name.  Use that.
//   2. The request matches an unamed gorilla mux router.  Munge the path
//      template such that templates like '/api/{org}/foo' come out as
//      'api_org_foo'.
//   3. The request doesn't match a mux route. Return "other"
// We do all this as we do not wish to emit high cardinality labels to
// prometheus.
func (i Instrument) getRouteName(r *http.Request) string {
	route := getRouteName(i.RouteMatcher, r)
	if route == "" {
		route = "other"
	}

	return route
}

func getRouteName(routeMatcher RouteMatcher, r *http.Request) string {
	var routeMatch mux.RouteMatch
	if routeMatcher != nil && routeMatcher.Match(r, &routeMatch) {
		if name := routeMatch.Route.GetName(); name != "" {
			return name
		}
		if tmpl, err := routeMatch.Route.GetPathTemplate(); err == nil {
			return MakeLabelValue(tmpl)
		}
	}

	return ""
}

var invalidChars = regexp.MustCompile(`[^a-zA-Z0-9]+`)

// MakeLabelValue converts a Gorilla mux path to a string suitable for use in
// a Prometheus label value.
func MakeLabelValue(path string) string {
	// Convert non-alnums to underscores.
	result := invalidChars.ReplaceAllString(path, "_")

	// Trim leading and trailing underscores.
	result = strings.Trim(result, "_")

	// Make it all lowercase
	result = strings.ToLower(result)

	// Special case.
	if result == "" {
		result = "root"
	}
	return result
}
