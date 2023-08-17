// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/logging_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/log"

	"github.com/stretchr/testify/require"
)

func TestBadWriteLogging(t *testing.T) {
	for _, tc := range []struct {
		err         error
		logContains []string
	}{{
		err:         context.Canceled,
		logContains: []string{"debug", "request cancelled: context canceled"},
	}, {
		err:         errors.New("yolo"),
		logContains: []string{"warn", "error: yolo"},
	}, {
		err:         nil,
		logContains: []string{"debug", "GET http://example.com/foo (200)"},
	}} {
		buf := bytes.NewBuffer(nil)
		logger := log.NewGoKitWithWriter(log.LogfmtFormat, buf)

		loggingMiddleware := Log{
			Log: logger,
		}
		handler := func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "<html><body>Hello World!</body></html>")
		}
		loggingHandler := loggingMiddleware.Wrap(http.HandlerFunc(handler))

		req := httptest.NewRequest("GET", "http://example.com/foo", nil)
		recorder := httptest.NewRecorder()

		w := errorWriter{
			err: tc.err,
			w:   recorder,
		}
		loggingHandler.ServeHTTP(w, req)

		for _, content := range tc.logContains {
			require.True(t, bytes.Contains(buf.Bytes(), []byte(content)))
		}
	}
}

func TestDisabledSuccessfulRequestsLogging(t *testing.T) {
	for _, tc := range []struct {
		err         error
		disableLog  bool
		logContains string
	}{
		{
			err:        nil,
			disableLog: false,
		}, {
			err:         nil,
			disableLog:  true,
			logContains: "",
		},
	} {
		buf := bytes.NewBuffer(nil)

		loggingMiddleware := Log{
			Log:                      log.NewGoKitWithWriter(log.LogfmtFormat, buf),
			DisableRequestSuccessLog: tc.disableLog,
		}

		handler := func(w http.ResponseWriter, r *http.Request) {
			_, err := io.WriteString(w, "<html><body>Hello World!</body></html>")
			require.NoError(t, err) //nolint:errcheck
		}
		loggingHandler := loggingMiddleware.Wrap(http.HandlerFunc(handler))

		req := httptest.NewRequest("GET", "http://example.com/foo", nil)
		recorder := httptest.NewRecorder()

		w := errorWriter{
			err: tc.err,
			w:   recorder,
		}
		loggingHandler.ServeHTTP(w, req)
		content := buf.String()

		if !tc.disableLog {
			require.Contains(t, content, "GET http://example.com/foo (200)")
		} else {
			require.NotContains(t, content, "(200)")
			require.Empty(t, content)
		}
	}
}

func TestLoggingRequestsAtInfoLevel(t *testing.T) {
	for _, tc := range []struct {
		err         error
		logContains []string
	}{{
		err:         context.Canceled,
		logContains: []string{"info", "request cancelled: context canceled"},
	}, {
		err:         nil,
		logContains: []string{"info", "GET http://example.com/foo (200)"},
	}} {
		buf := bytes.NewBuffer(nil)

		loggingMiddleware := Log{
			Log:                   log.NewGoKitWithWriter(log.LogfmtFormat, buf),
			LogRequestAtInfoLevel: true,
		}
		handler := func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "<html><body>Hello World!</body></html>")
		}
		loggingHandler := loggingMiddleware.Wrap(http.HandlerFunc(handler))

		req := httptest.NewRequest("GET", "http://example.com/foo", nil)
		recorder := httptest.NewRecorder()

		w := errorWriter{
			err: tc.err,
			w:   recorder,
		}
		loggingHandler.ServeHTTP(w, req)

		for _, content := range tc.logContains {
			require.True(t, bytes.Contains(buf.Bytes(), []byte(content)))
		}
	}
}

func TestLoggingRequestWithExcludedHeaders(t *testing.T) {
	defaultHeaders := []string{"Authorization", "Cookie", "X-Csrf-Token"}
	for _, tc := range []struct {
		name              string
		setHeaderList     []string
		excludeHeaderList []string
		mustNotContain    []string
	}{
		{
			name:           "Default excluded headers are excluded",
			setHeaderList:  defaultHeaders,
			mustNotContain: defaultHeaders,
		},
		{
			name:              "Extra configured header is also excluded",
			setHeaderList:     append(defaultHeaders, "X-Secret-Header"),
			excludeHeaderList: []string{"X-Secret-Header"},
			mustNotContain:    append(defaultHeaders, "X-Secret-Header"),
		},
		{
			name:              "Multiple extra configured headers are also excluded",
			setHeaderList:     append(defaultHeaders, "X-Secret-Header", "X-Secret-Header-2"),
			excludeHeaderList: []string{"X-Secret-Header", "X-Secret-Header-2"},
			mustNotContain:    append(defaultHeaders, "X-Secret-Header", "X-Secret-Header-2"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)

			loggingMiddleware := NewLogMiddleware(log.NewGoKitWithWriter(log.LogfmtFormat, buf), true, false, nil, tc.excludeHeaderList)

			handler := func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, "<html><body>Hello world!</body></html>")
			}
			loggingHandler := loggingMiddleware.Wrap(http.HandlerFunc(handler))

			req := httptest.NewRequest("GET", "http://example.com/foo", nil)
			for _, header := range tc.setHeaderList {
				req.Header.Set(header, header)
			}

			recorder := httptest.NewRecorder()
			loggingHandler.ServeHTTP(recorder, req)

			output := buf.String()
			for _, header := range tc.mustNotContain {
				require.NotContains(t, output, header)
			}
		})
	}
}

type errorWriter struct {
	err error

	w http.ResponseWriter
}

func (e errorWriter) Header() http.Header {
	return e.w.Header()
}

func (e errorWriter) WriteHeader(statusCode int) {
	e.w.WriteHeader(statusCode)
}

func (e errorWriter) Write(b []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}

	return e.w.Write(b)
}
