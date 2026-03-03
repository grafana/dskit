package runtimeconfig

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// provider reads raw bytes from a config source.
type provider interface {
	Read(ctx context.Context, path string) ([]byte, error)
}

// fileProvider reads config from the local filesystem.
type fileProvider struct{}

func (f *fileProvider) Read(_ context.Context, path string) ([]byte, error) {
	return os.ReadFile(path)
}

// httpProvider fetches config from HTTP/HTTPS URLs with RED metrics.
type httpProvider struct {
	client          *http.Client
	requestDuration *prometheus.HistogramVec
}

func newHTTPProvider(client *http.Client, registerer prometheus.Registerer) *httpProvider {
	return &httpProvider{
		client: client,
		requestDuration: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "runtime_config_http_request_duration_seconds",
			Help:    "Time spent fetching runtime config from HTTP URLs.",
			Buckets: prometheus.DefBuckets,
			// Use defaults recommended by Prometheus for native histograms.
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, []string{"url", "status_code"}),
	}
}

func (h *httpProvider) Read(ctx context.Context, url string) ([]byte, error) {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		h.requestDuration.WithLabelValues(url, "error").Observe(time.Since(start).Seconds())
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	statusCode := strconv.Itoa(resp.StatusCode)

	if resp.StatusCode/100 != 2 {
		h.requestDuration.WithLabelValues(url, statusCode).Observe(time.Since(start).Seconds())
		return nil, &httpError{statusCode: resp.StatusCode, url: url}
	}

	if err != nil {
		h.requestDuration.WithLabelValues(url, "error").Observe(time.Since(start).Seconds())
		return nil, fmt.Errorf("read response body: %w", err)
	}

	h.requestDuration.WithLabelValues(url, statusCode).Observe(time.Since(start).Seconds())
	return body, nil
}

type httpError struct {
	statusCode int
	url        string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("HTTP %d from %s", e.statusCode, e.url)
}

func isURL(path string) bool {
	return strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://")
}
