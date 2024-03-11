package middleware

import (
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

// TestTimeoutMiddleware tests the following behavior of the timeout middleware:
// - server write timeout is disabled
// - server returns 503 Service Unavailable with the proivded message
// - the context is cancelled
func TestTimeoutMiddleware(t *testing.T) {
	const (
		serverWriteTimeout    = time.Second
		httpMiddlewareTimeout = 3 * time.Second
		httpMiddlewareMessage = "foo"
	)

	timeoutMiddleware := NewTimeoutMiddleware(httpMiddlewareTimeout, httpMiddlewareMessage, log.NewNopLogger())

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	httpServer := &http.Server{
		WriteTimeout: serverWriteTimeout,
		Handler: Merge(timeoutMiddleware).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// block until the context is done. this is cancelled by the timeout handler
			<-r.Context().Done()

			w.WriteHeader(204)

			// the context should be in error b/c the timeouthandler cancelled it
			require.Error(t, r.Context().Err())
		})),
	}

	go func() {
		require.NoError(t, httpServer.Serve(listener))
	}()

	start := time.Now()
	req, err := http.NewRequest("GET", "http://"+listener.Addr().String(), nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, httpMiddlewareMessage, string(body))
	defer resp.Body.Close()

	// confirm that we waited at least the middleware timeout (and not the server write timeout)
	require.GreaterOrEqual(t, time.Since(start), httpMiddlewareTimeout)
}
