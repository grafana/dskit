package middleware

import (
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/test"
)

func TestZeroResponseLogger(t *testing.T) {
	const body = "Hello"

	s := http.Server{
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = writer.Write([]byte(body))
		}),
		ReadTimeout:       0,
		ReadHeaderTimeout: 1 * time.Second,
	}

	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	logBuf := concurrency.SyncBuffer{}
	zrl := NewZeroResponseListener(ln, log.NewLogfmtLogger(&logBuf))

	go func() {
		_ = s.Serve(zrl)
	}()
	t.Cleanup(func() {
		_ = ln.Close()
	})

	t.Run("multiple quick requests", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			r, err := http.Get("http://" + ln.Addr().String())
			require.NoError(t, err)
			defer func() { _ = r.Body.Close() }()

			require.Equal(t, 200, r.StatusCode)
			read, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, body, string(read))
		}
	})

	t.Run("slow request", func(t *testing.T) {
		logBuf.Reset()

		c, err := net.Dial("tcp", ln.Addr().String())
		require.NoError(t, err)
		defer func() {
			_ = c.Close()
		}()

		_, err = c.Write([]byte("GET / HTTP/1.1\r\nHost: somehost:12345\r\n")) // unfinished request
		require.NoError(t, err)

		// HTTP server will close the connection in 1 second (see above config), without any response.
		read, err := io.ReadAll(c)
		require.NoError(t, err)
		require.Equal(t, "", string(read))

		// Wait a bit until connection is closed and log message logged.
		test.Poll(t, 1*time.Second, true, func() interface{} {
			return logBuf.String() != ""
		})

		require.Contains(t, logBuf.String(), `msg="read timeout, connection closed with no response"`)
		require.Contains(t, logBuf.String(), `read="\"GET / HTTP/1.1\\r\\nHost: somehost:12345\\r\\n\""`)
		require.Contains(t, logBuf.String(), `remote=`)
	})

	t.Run("slow request with multiple writes", func(t *testing.T) {
		logBuf.Reset()

		c, err := net.Dial("tcp", ln.Addr().String())
		require.NoError(t, err)
		defer func() {
			_ = c.Close()
		}()

		_, err = c.Write([]byte("GET / HTTP/1.1\r\n"))
		require.NoError(t, err)
		_, err = c.Write([]byte("X-Org-ScopeID: 54321\r\n"))
		require.NoError(t, err)
		_, err = c.Write([]byte("Host: somehost:12345\r\n"))
		require.NoError(t, err)

		// HTTP server will close the connection in 1 second (see above config), without any response.
		read, err := io.ReadAll(c)
		require.NoError(t, err)
		require.Equal(t, "", string(read))

		// Wait a bit until connection is closed and log message logged.
		test.Poll(t, 1*time.Second, true, func() interface{} {
			return logBuf.String() != ""
		})

		require.Contains(t, logBuf.String(), `msg="read timeout, connection closed with no response"`)
		require.Contains(t, logBuf.String(), `read="\"GET / HTTP/1.1\\r\\nX-Org-ScopeID: 54321\\r\\nHost: somehost:12345\\r\\n\""`)
		require.Contains(t, logBuf.String(), `remote=`)
	})

	t.Run("slow request with authorization header", func(t *testing.T) {
		logBuf.Reset()

		c, err := net.Dial("tcp", ln.Addr().String())
		require.NoError(t, err)
		defer func() {
			_ = c.Close()
		}()

		_, err = c.Write([]byte("GET / HTTP/1.1\r\n"))
		require.NoError(t, err)
		_, err = c.Write([]byte("X-Org-ScopeID: 54321\r\n"))
		require.NoError(t, err)
		_, err = c.Write([]byte("Authorization: Basic YWFhOmJiYg==\r\n"))
		require.NoError(t, err)

		// HTTP server will close the connection in 1 second (see above config), without any response.
		read, err := io.ReadAll(c)
		require.NoError(t, err)
		require.Equal(t, "", string(read))

		// Wait a bit until connection is closed and log message logged.
		test.Poll(t, 1*time.Second, true, func() interface{} {
			return logBuf.String() != ""
		})

		require.Contains(t, logBuf.String(), `msg="read timeout, connection closed with no response"`)
		require.Contains(t, logBuf.String(), `read="\"GET / HTTP/1.1\\r\\nX-Org-ScopeID: 54321\\r\\nAuthorization: Basic ***\\r\\n\""`)
		require.Contains(t, logBuf.String(), `remote=`)
	})
}
