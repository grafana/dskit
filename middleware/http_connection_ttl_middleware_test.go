package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	conn1 = "1.2.3.4:5"
	conn2 = "1.2.3.4:6"
)

func TestNewHTTPConnectionTTLMiddleware(t *testing.T) {
	testCases := map[string]struct {
		minTTL                       time.Duration
		maxTTL                       time.Duration
		idleConnectionTimeout        time.Duration
		idleConnectionCheckFrequency time.Duration
		expectedErr                  error
	}{
		"if limiter is disabled, return no error when idleConnectionTimeout <= 0": {
			minTTL:                       0,
			maxTTL:                       0,
			idleConnectionTimeout:        0,
			idleConnectionCheckFrequency: time.Second,
			expectedErr:                  nil,
		},
		"if limiter is disabled, return no error when idleConnectionCheckFrequency <= 0": {
			minTTL:                       0,
			maxTTL:                       0,
			idleConnectionTimeout:        10 * time.Second,
			idleConnectionCheckFrequency: 0,
			expectedErr:                  nil,
		},
		"return errMinLessOrEqualThanMax when minTTL > maxTTL": {
			minTTL:                       10,
			maxTTL:                       0,
			idleConnectionTimeout:        10 * time.Second,
			idleConnectionCheckFrequency: time.Second,
			expectedErr:                  errMinLessOrEqualThanMax,
		},
		"if limiter is enabled, return errIdleConnectionCheckFrequencyMustBePositive when idleConnectionCheckFrequency <= 0": {
			minTTL:                       0,
			maxTTL:                       5,
			idleConnectionTimeout:        10 * time.Second,
			idleConnectionCheckFrequency: 0,
			expectedErr:                  errIdleConnectionCheckFrequencyMustBePositive,
		},
		"if limiter is enabled, return no error if minTTL < maxTTL and idleConnectionTimeout and idleConnectionCheckFrequency are positive": {
			minTTL:                       5,
			maxTTL:                       10,
			idleConnectionTimeout:        10 * time.Second,
			idleConnectionCheckFrequency: time.Second,
			expectedErr:                  nil,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m, err := NewHTTPConnectionTTLMiddleware(t.Context(), tc.minTTL, tc.maxTTL, tc.idleConnectionCheckFrequency, prometheus.NewRegistry())
			if tc.expectedErr == nil {
				require.NoError(t, err)
				require.NotNil(t, m)
			} else {
				require.ErrorIs(t, err, tc.expectedErr)
				require.Nil(t, m)
			}
		})
	}

}

func TestHTTPConnectionTTLMiddleware_CalculateTTL(t *testing.T) {
	const addr = "1.2.3.4:567"
	testCases := map[string]struct {
		minTTL time.Duration
		maxTTL time.Duration
		check  func(time.Duration, time.Duration, time.Duration)
	}{
		"calculate connection's TTL between min and max TTLs": {
			minTTL: 10 * time.Second,
			maxTTL: 100 * time.Second,
			check: func(minTTL, maxTTL, actualTTL time.Duration) {
				require.LessOrEqual(t, minTTL, actualTTL)
				require.GreaterOrEqual(t, maxTTL, actualTTL)
			},
		},
		"when min TTL and max TTL are 0, calculated TTL is 0": {
			minTTL: 0,
			maxTTL: 0,
			check: func(_, _, actualTTL time.Duration) {
				require.Equal(t, 0*time.Second, actualTTL)
			},
		},
		"when min and max TTLs are equal, calculated TTL returns the same value": {
			minTTL: 10 * time.Second,
			maxTTL: 10 * time.Second,
			check: func(_, _, actualTTL time.Duration) {
				require.Equal(t, 10*time.Second, actualTTL)
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			m, err := NewHTTPConnectionTTLMiddleware(t.Context(), tc.minTTL, tc.maxTTL, time.Second, reg)
			require.NoError(t, err)
			require.NotNil(t, m)

			limiterMiddleware, ok := m.(*httpConnectionTTLMiddleware)
			require.True(t, ok)
			require.NotNil(t, limiterMiddleware)

			requestLimit := limiterMiddleware.calculateTTL(addr)
			tc.check(tc.minTTL, tc.maxTTL, requestLimit)
		})
	}
}

func TestHTTPConnectionTTLMiddleware_Wrap(t *testing.T) {
	metricNames := []string{
		"open_connections_with_ttl_total",
		"closed_connections_with_ttl_total",
	}
	tcs := map[string]struct {
		maxTTL                         time.Duration
		minTTL                         time.Duration
		requestRemoteAddresses         []string
		waitingTimes                   []time.Duration
		expectedConnectionHeaderValues []string
		expectedMetrics                string
	}{
		"do not add Connection header when there is no maxTTL": {
			maxTTL:                         0,
			minTTL:                         0,
			requestRemoteAddresses:         []string{conn1, conn2},
			expectedConnectionHeaderValues: []string{"", ""},
			expectedMetrics: `
				# HELP open_connections_with_ttl_total Number of connections that connection with TTL middleware started tracking
				# TYPE open_connections_with_ttl_total counter
				open_connections_with_ttl_total 0
			`,
		},
		"do not add Connection header when connection TTL is not reached": {
			maxTTL:                         30 * time.Millisecond,
			minTTL:                         10 * time.Millisecond,
			requestRemoteAddresses:         []string{conn1, conn2, conn1, conn2},
			expectedConnectionHeaderValues: []string{"", "", "", ""},
			expectedMetrics: `
				# HELP open_connections_with_ttl_total Number of connections that connection with TTL middleware started tracking
				# TYPE open_connections_with_ttl_total counter
				open_connections_with_ttl_total 2
			`,
		},
		"add Connection: close header when connection TTL is reached": {
			maxTTL:                         200 * time.Millisecond,
			minTTL:                         100 * time.Millisecond,
			requestRemoteAddresses:         []string{conn1, conn2, conn1, conn1, conn2},
			waitingTimes:                   []time.Duration{0, 0, 0, 250 * time.Millisecond, 0},
			expectedConnectionHeaderValues: []string{"", "", "", connectionHeaderCloseValue, connectionHeaderCloseValue},
			expectedMetrics: `
				# HELP open_connections_with_ttl_total Number of connections that connection with TTL middleware started tracking
				# TYPE open_connections_with_ttl_total counter
				open_connections_with_ttl_total 2
				# HELP closed_connections_with_ttl_total Number of connections that connection with TTL middleware closed or stopped tracking
				# TYPE closed_connections_with_ttl_total counter
				closed_connections_with_ttl_total{reason="limit"} 2
			`,
		},
	}
	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			// create request per connection limiter middleware
			m, err := NewHTTPConnectionTTLMiddleware(t.Context(), tc.minTTL, tc.maxTTL, 1*time.Second, reg)
			require.NoError(t, err)

			// declare default handler
			hnd := m.Wrap(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(http.StatusOK)
			}))

			// spawn active requests
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			for i, remoteAddr := range tc.requestRemoteAddresses {
				w := httptest.NewRecorder()
				req, err := createRequestWith(ctx, http.MethodGet, "/", remoteAddr)
				require.NoError(t, err)
				if tc.waitingTimes != nil && tc.waitingTimes[i] != 0 {
					time.Sleep(tc.waitingTimes[i])
				}
				hnd.ServeHTTP(w, req)
				require.Equal(t, tc.expectedConnectionHeaderValues[i], w.Header().Get(connectionHeaderKey))
				require.True(t, checkHTTPConnectionTTL(t, m, remoteAddr, tc.expectedConnectionHeaderValues[i] != connectionHeaderCloseValue))
			}
			assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(tc.expectedMetrics), metricNames...))
		})
	}
}

func TestHTTPConnectionTTLMiddleware_RemoveIdleExpiredConnections(t *testing.T) {
	const idleConnectionCheckFrequency = 10 * time.Millisecond
	metricNames := []string{
		"open_connections_with_ttl_total",
		"closed_connections_with_ttl_total",
	}
	expectedMetrics := `
		# HELP open_connections_with_ttl_total Number of connections that connection with TTL middleware started tracking
		# TYPE open_connections_with_ttl_total counter
		open_connections_with_ttl_total 1
		# HELP closed_connections_with_ttl_total Number of connections that connection with TTL middleware closed or stopped tracking
		# TYPE closed_connections_with_ttl_total counter
		closed_connections_with_ttl_total{reason="idle timeout"} 1
	`
	reg := prometheus.NewRegistry()
	m, err := NewHTTPConnectionTTLMiddleware(t.Context(), 1*time.Second, 2*time.Second, idleConnectionCheckFrequency, reg)
	require.NoError(t, err)

	// declare default handler
	hnd := m.Wrap(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))

	// spawn active requests
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	w := httptest.NewRecorder()
	req, err := createRequestWith(ctx, http.MethodGet, "/", conn1)
	require.NoError(t, err)
	hnd.ServeHTTP(w, req)
	require.NotEqual(t, connectionHeaderCloseValue, w.Header().Get(connectionHeaderKey))
	require.True(t, checkHTTPConnectionTTL(t, m, conn1, true))

	require.Eventually(t, func() bool {
		return checkHTTPConnectionTTL(t, m, conn1, false)
	}, 3*time.Second, idleConnectionCheckFrequency)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
}

func TestHTTPConnectionTTLMiddleware_RemoveIdleExpiredConnectionsKeepsActiveConnections(t *testing.T) {
	const (
		minTTL             = 500 * time.Millisecond
		maxTTL             = 1000 * time.Millisecond
		requestFrequency   = 10 * time.Millisecond
		idleCheckFrequency = 20 * time.Millisecond
	)
	metricNames := []string{
		"open_connections_with_ttl_total",
		"closed_connections_with_ttl_total",
	}
	expectedMetrics := `
		# HELP open_connections_with_ttl_total Number of connections that connection with TTL middleware started tracking
		# TYPE open_connections_with_ttl_total counter
		open_connections_with_ttl_total 1
		# HELP closed_connections_with_ttl_total Number of connections that connection with TTL middleware closed or stopped tracking
		# TYPE closed_connections_with_ttl_total counter
		closed_connections_with_ttl_total{reason="limit"} 1
		closed_connections_with_ttl_total{reason="idle timeout"} 0
	`

	reg := prometheus.NewRegistry()
	m, err := NewHTTPConnectionTTLMiddleware(t.Context(), minTTL, maxTTL, idleCheckFrequency, reg)
	require.NoError(t, err)

	rpcMiddleware, ok := m.(*httpConnectionTTLMiddleware)
	require.True(t, ok)

	// declare default handler
	hnd := m.Wrap(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))

	// spawn active requests
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	requestTicker := time.NewTicker(requestFrequency)
	defer requestTicker.Stop()
	var wg sync.WaitGroup
	t.Cleanup(wg.Wait)
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-requestTicker.C:
				w := httptest.NewRecorder()
				req, err := createRequestWith(ctx, http.MethodGet, "/", conn1)
				require.NoError(t, err)
				hnd.ServeHTTP(w, req)
				if w.Header().Get(connectionHeaderKey) == connectionHeaderCloseValue {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	hasConnection := false
	require.Eventually(t, func() bool {
		if !hasConnection {
			hasConnection = checkHTTPConnectionTTL(t, m, conn1, true)
			if !hasConnection {
				// Have to wait until the connection gets cached.
				return false
			}

			// The connection is cached - proceed.
			rpcMiddleware.removeIdleExpiredConnections()
		}

		// Wait until connection is expired from cache (reason="limit").
		return checkHTTPConnectionTTL(t, m, conn1, false)
	}, 2*maxTTL, idleCheckFrequency)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
}

func checkHTTPConnectionTTL(t *testing.T, m Interface, conn string, shouldConnBeActive bool) bool {
	rpcMiddleware, ok := m.(*httpConnectionTTLMiddleware)
	assert.True(t, ok)

	if rpcMiddleware.maxTTL <= 0 {
		return true
	}

	rpcMiddleware.connectionsMu.Lock()
	defer rpcMiddleware.connectionsMu.Unlock()
	_, ok = rpcMiddleware.connections[conn]
	return ok == shouldConnBeActive
}

func createRequestWith(ctx context.Context, method string, url string, remoteAddr string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	req.RemoteAddr = remoteAddr
	return req, nil
}
