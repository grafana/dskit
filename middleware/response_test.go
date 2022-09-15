package middleware

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBadResponseLoggingWriter(t *testing.T) {
	for _, tc := range []struct {
		statusCode int
		data       string
		expected   string
	}{
		{http.StatusOK, "", ""},
		{http.StatusOK, "some data", ""},
		{http.StatusUnprocessableEntity, "unprocessable", ""},
		{http.StatusInternalServerError, "", ""},
		{http.StatusInternalServerError, "bad juju", "bad juju\n"},
	} {
		w := httptest.NewRecorder()
		var buf bytes.Buffer
		wrapped := newBadResponseLoggingWriter(w, &buf)
		switch {
		case tc.data == "":
			wrapped.WriteHeader(tc.statusCode)
		case tc.statusCode < 300 && tc.data != "":
			wrapped.WriteHeader(tc.statusCode)
			wrapped.Write([]byte(tc.data))
		default:
			http.Error(wrapped, tc.data, tc.statusCode)
		}
		if wrapped.getStatusCode() != tc.statusCode {
			t.Errorf("Wrong status code: have %d want %d", wrapped.getStatusCode(), tc.statusCode)
		}
		data := string(buf.Bytes())
		if data != tc.expected {
			t.Errorf("Wrong data: have %q want %q", data, tc.expected)
		}
	}
}

// nonFlushingResponseWriter implements http.ResponseWriter but does not implement http.Flusher
type nonFlushingResponseWriter struct{}

func (rw *nonFlushingResponseWriter) Header() http.Header {
	return nil
}

func (rw *nonFlushingResponseWriter) Write(_ []byte) (int, error) {
	return -1, nil
}

func (rw *nonFlushingResponseWriter) WriteHeader(_ int) {
}

func TestBadResponseLoggingWriter_WithAndWithoutFlusher(t *testing.T) {
	var buf bytes.Buffer

	nf := newBadResponseLoggingWriter(&nonFlushingResponseWriter{}, &buf)

	_, ok := nf.(http.Flusher)
	if ok {
		t.Errorf("Should not be able to cast nf as an http.Flusher")
	}

	rec := httptest.NewRecorder()
	f := newBadResponseLoggingWriter(rec, &buf)

	ff, ok := f.(http.Flusher)
	if !ok {
		t.Errorf("Should be able to cast f as an http.Flusher")
	}

	ff.Flush()
	if !rec.Flushed {
		t.Errorf("Flush should have worked but did not")
	}
}
