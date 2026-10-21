package server

import (
	"flag"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
)

func TestMetricsCacheDuration(t *testing.T) {
	for _, duration := range []time.Duration{0, 15 * time.Second} {
		t.Run(duration.String(), func(t *testing.T) {
			var cfg Config
			flags := flag.NewFlagSet("test", flag.ContinueOnError)
			cfg.RegisterFlags(flags)
			require.Zero(t, cfg.MetricsCacheDuration)
			require.NoError(t, flags.Parse([]string{"-server.metrics-cache-duration=" + duration.String()}))
			require.Equal(t, duration, cfg.MetricsCacheDuration)
			require.NoError(t, cfg.Validate())
			cfg.PathPrefix = "/test"
			setAutoAssignedPorts("tcp", &cfg)
			reg := prometheus.NewRegistry()
			cfg.Registerer, cfg.Gatherer = reg, reg
			gauge := promauto.With(reg).NewGauge(prometheus.GaugeOpts{Name: "test_cache_value", Help: "Value."})
			srv, err := New(cfg)
			require.NoError(t, err)
			t.Cleanup(func() {
				// These listeners were opened by New but never served.
				require.NoError(t, srv.httpListener.Close())
				require.NoError(t, srv.grpcListener.Close())
				srv.Shutdown()
			})
			scrape := func() string {
				w := httptest.NewRecorder()
				srv.HTTP.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/test/metrics", nil))
				require.Equal(t, http.StatusOK, w.Code)
				return w.Body.String()
			}
			gauge.Set(1)
			require.Contains(t, scrape(), "test_cache_value 1\n")
			gauge.Set(2)
			if duration > 0 {
				require.Contains(t, scrape(), "test_cache_value 1\n")
			} else {
				require.Contains(t, scrape(), "test_cache_value 2\n")
			}
		})
	}
	t.Run("negative duration", func(t *testing.T) {
		cfg := Config{MetricsCacheDuration: -time.Second}
		require.EqualError(t, cfg.Validate(), "server.metrics-cache-duration must not be negative")
	})
}
