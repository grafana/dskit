package middleware

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/logging"
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
		logContains: []string{"warning", "error: yolo"},
	}, {
		err:         nil,
		logContains: []string{"debug", "GET http://example.com/foo (200)"},
	}} {
		buf := bytes.NewBuffer(nil)
		logrusLogger := logrus.New()
		logrusLogger.Out = buf
		logrusLogger.Level = logrus.DebugLevel

		loggingMiddleware := Log{
			Log: logging.Logrus(logrusLogger),
		}
		handler := func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "<html><body>Hello World!</body></html>")
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
