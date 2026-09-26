// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/v1.10.0/pkg/util/http.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

// Package httputil provides helpers for writing HTTP responses.
package httputil

import (
	"encoding/json"
	"html/template"
	"net/http"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// MarshalFunc encodes a value. YAML helpers require a non-nil function so callers
// can retain their YAML library's formatting and custom marshaler support.
type MarshalFunc func(any) ([]byte, error)

// WriteJSONResponse writes JSON without a trailing newline, or a 500 response if encoding fails.
func WriteJSONResponse(w http.ResponseWriter, v any) {
	writeMarshaledResponse(w, v, "application/json", json.Marshal)
}

// WriteYAMLResponse writes YAML using marshal, or a 500 response if encoding fails.
// The text/plain content type allows browsers to display the response.
func WriteYAMLResponse(w http.ResponseWriter, v any, marshal MarshalFunc) {
	writeMarshaledResponse(w, v, "text/plain; charset=utf-8", marshal)
}

func writeMarshaledResponse(w http.ResponseWriter, v any, contentType string, marshal MarshalFunc) {
	w.Header().Set("Content-Type", contentType)
	data, err := marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// A write failure cannot be replaced with a new HTTP status once headers are sent.
	_, _ = w.Write(data)
}

// WriteTextResponse writes a text/plain response without adding a newline.
func WriteTextResponse(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(message))
}

// WriteHTMLResponse writes a text/html response. The caller must supply safe HTML.
func WriteHTMLResponse(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/html")
	_, _ = w.Write([]byte(message))
}

// RenderHTTPResponse writes JSON if Accept contains "application/json", otherwise
// it executes t directly into w. An empty htmlContentType leaves content detection
// or an existing header to the response writer. Template errors use http.Error;
// if the template has already written output, the response status is already committed.
func RenderHTTPResponse(w http.ResponseWriter, v any, t *template.Template, r *http.Request, htmlContentType string) {
	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		WriteJSONResponse(w, v)
		return
	}
	if htmlContentType != "" {
		w.Header().Set("Content-Type", htmlContentType)
	}
	if err := t.Execute(w, v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// StreamWriteYAMLResponse writes each value from iter using marshal until the
// channel closes or a write fails. It logs and skips marshal errors, and logs
// write failures. Encoded bytes are written without adding document separators or flushing.
func StreamWriteYAMLResponse(w http.ResponseWriter, iter <-chan any, logger log.Logger, marshal MarshalFunc) {
	w.Header().Set("Content-Type", "application/yaml")
	for v := range iter {
		data, err := marshal(v)
		if err != nil {
			level.Error(logger).Log("msg", "yaml marshal failed", "err", err)
			continue
		}
		if _, err := w.Write(data); err != nil {
			level.Error(logger).Log("msg", "write http response failed", "err", err)
			return
		}
	}
}
