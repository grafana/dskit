package httputil

import (
	"bytes"
	"errors"
	"html/template"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestWriteResponses(t *testing.T) {
	for _, tc := range []struct {
		name              string
		write             func(http.ResponseWriter)
		contentType, body string
		status            int
	}{
		{"json", func(w http.ResponseWriter) { WriteJSONResponse(w, map[string]any{"html": "<tag>", "number": 1}) }, "application/json", `{"html":"\u003ctag\u003e","number":1}`, 200},
		{"json nil", func(w http.ResponseWriter) { WriteJSONResponse(w, nil) }, "application/json", "null", 200},
		{"json error", func(w http.ResponseWriter) { WriteJSONResponse(w, make(chan int)) }, "text/plain; charset=utf-8", "json: unsupported type: chan int\n", 500},
		{"yaml", func(w http.ResponseWriter) {
			WriteYAMLResponse(w, "input", func(v any) ([]byte, error) {
				require.Equal(t, "input", v)
				return []byte("quoted: 'yes'\nlist:\n  - value\n"), nil
			})
		}, "text/plain; charset=utf-8", "quoted: 'yes'\nlist:\n  - value\n", 200},
		{"yaml error", func(w http.ResponseWriter) {
			WriteYAMLResponse(w, nil, func(any) ([]byte, error) { return []byte("discard"), errors.New("encode failed") })
		}, "text/plain; charset=utf-8", "encode failed\n", 500},
		{"text", func(w http.ResponseWriter) { WriteTextResponse(w, "message") }, "text/plain", "message", 200},
		{"empty text", func(w http.ResponseWriter) { WriteTextResponse(w, "") }, "text/plain", "", 200},
		{"html", func(w http.ResponseWriter) { WriteHTMLResponse(w, "<b>message</b>") }, "text/html", "<b>message</b>", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			w.Header().Set("Content-Type", "overridden")
			tc.write(w)
			requireResponse(t, w, tc.status, tc.contentType, tc.body)
			if tc.status == 500 {
				require.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
			}
		})
	}
}

func TestWriteResponsePreservesCommittedStatus(t *testing.T) {
	for _, write := range []func(http.ResponseWriter){
		func(w http.ResponseWriter) { WriteJSONResponse(w, "value") },
		func(w http.ResponseWriter) { WriteTextResponse(w, "value") },
		func(w http.ResponseWriter) { WriteHTMLResponse(w, "value") },
		func(w http.ResponseWriter) {
			WriteYAMLResponse(w, nil, func(any) ([]byte, error) { return []byte("value"), nil })
		},
	} {
		w := httptest.NewRecorder()
		w.Header().Set("Content-Type", "already-sent")
		w.WriteHeader(http.StatusAccepted)
		write(w)
		response := w.Result()
		require.NoError(t, response.Body.Close())
		require.Equal(t, http.StatusAccepted, response.StatusCode)
		require.Equal(t, "already-sent", response.Header.Get("Content-Type"))
	}
}

func TestRenderHTTPResponse(t *testing.T) {
	tmpl := template.Must(template.New("page").Parse("<p>{{.}}</p>"))
	for _, tc := range []struct{ name, accept, configured, existing, contentType, body string }{
		{"mimir html", "text/html", "text/html; charset=utf-8", "", "text/html; charset=utf-8", "<p>&lt;tag&gt;</p>"},
		{"loki tempo html", "", "", "", "text/html; charset=utf-8", "<p>&lt;tag&gt;</p>"},
		{"existing header", "", "", "custom/type", "custom/type", "<p>&lt;tag&gt;</p>"},
		{"json", "application/json", "text/html; charset=utf-8", "", "application/json", `"\u003ctag\u003e"`},
		{"accept substring", "text/html, application/json;q=0", "", "", "application/json", `"\u003ctag\u003e"`},
		{"accept is case sensitive", "APPLICATION/JSON", "", "", "text/html; charset=utf-8", "<p>&lt;tag&gt;</p>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			if tc.existing != "" {
				w.Header().Set("Content-Type", tc.existing)
			}
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Accept", tc.accept)
			RenderHTTPResponse(w, "<tag>", tmpl, r, tc.configured)
			requireResponse(t, w, 200, tc.contentType, tc.body)
		})
	}
}

func TestRenderHTTPResponseContentDetection(t *testing.T) {
	for _, contentType := range []string{"", "text/html; charset=utf-8"} {
		w := httptest.NewRecorder()
		tmpl := template.Must(template.New("page").Parse("plain text"))
		RenderHTTPResponse(w, nil, tmpl, httptest.NewRequest("GET", "/", nil), contentType)
		want := contentType
		if want == "" {
			want = "text/plain; charset=utf-8"
		}
		requireResponse(t, w, 200, want, "plain text")
	}
}

func TestRenderHTTPResponseTemplateError(t *testing.T) {
	for _, prefix := range []string{"", "<p>partial</p>"} {
		t.Run(prefix, func(t *testing.T) {
			tmpl := template.Must(template.New("page").Funcs(template.FuncMap{
				"fail": func() (string, error) { return "", errors.New("render failed") },
			}).Parse(prefix + "{{fail}}"))
			w := httptest.NewRecorder()
			RenderHTTPResponse(w, nil, tmpl, httptest.NewRequest("GET", "/", nil), "text/html; charset=utf-8")
			response := w.Result()
			require.NoError(t, response.Body.Close())
			if prefix == "" {
				require.Equal(t, 500, response.StatusCode)
				require.Equal(t, "text/plain; charset=utf-8", response.Header.Get("Content-Type"))
			} else {
				require.Equal(t, 200, response.StatusCode)
				require.Equal(t, "text/html; charset=utf-8", response.Header.Get("Content-Type"))
			}
			require.Contains(t, w.Body.String(), "error calling fail: render failed\n")
			require.Contains(t, w.Body.String(), prefix)
		})
	}
}

func TestStreamWriteYAMLResponse(t *testing.T) {
	var logs bytes.Buffer
	iter := make(chan any, 3)
	iter <- "first\n"
	iter <- "bad"
	iter <- "second\n"
	close(iter)
	w := httptest.NewRecorder()
	StreamWriteYAMLResponse(w, iter, log.NewLogfmtLogger(&logs), func(v any) ([]byte, error) {
		if v == "bad" {
			return nil, errors.New("encode failed")
		}
		return []byte(v.(string)), nil
	})
	requireResponse(t, w, 200, "application/yaml", "first\nsecond\n")
	require.Equal(t, "level=error msg=\"yaml marshal failed\" err=\"encode failed\"\n", logs.String())
}

func TestStreamWriteYAMLResponseEmpty(t *testing.T) {
	iter := make(chan any)
	close(iter)
	w := httptest.NewRecorder()
	StreamWriteYAMLResponse(w, iter, log.NewNopLogger(), func(any) ([]byte, error) { t.Fatal("unexpected marshal"); return nil, nil })
	requireResponse(t, w, 200, "application/yaml", "")
}

func TestResponseWriteErrors(t *testing.T) {
	for _, write := range []func(http.ResponseWriter){
		func(w http.ResponseWriter) { WriteJSONResponse(w, "value") },
		func(w http.ResponseWriter) { WriteTextResponse(w, "value") },
		func(w http.ResponseWriter) { WriteHTMLResponse(w, "value") },
		func(w http.ResponseWriter) {
			WriteYAMLResponse(w, nil, func(any) ([]byte, error) { return []byte("value"), nil })
		},
	} {
		w := &failedWriter{header: make(http.Header)}
		write(w)
		require.Equal(t, 1, w.writes)
		require.Empty(t, w.statuses, "must not try to send an error after a write failure")
	}
	var logs bytes.Buffer
	w := &failedWriter{header: make(http.Header)}
	iter := make(chan any, 2)
	iter <- "first"
	iter <- "second"
	close(iter)
	StreamWriteYAMLResponse(w, iter, log.NewLogfmtLogger(&logs), func(v any) ([]byte, error) { return []byte(v.(string)), nil })
	require.Equal(t, 1, w.writes)
	require.Len(t, iter, 1)
	require.Equal(t, "level=error msg=\"write http response failed\" err=\"write failed\"\n", logs.String())
}

type failedWriter struct {
	header   http.Header
	writes   int
	statuses []int
}

func (w *failedWriter) Header() http.Header       { return w.header }
func (w *failedWriter) WriteHeader(status int)    { w.statuses = append(w.statuses, status) }
func (w *failedWriter) Write([]byte) (int, error) { w.writes++; return 0, errors.New("write failed") }

func requireResponse(t *testing.T, w *httptest.ResponseRecorder, status int, contentType, body string) {
	t.Helper()
	response := w.Result()
	require.NoError(t, response.Body.Close())
	require.Equal(t, status, response.StatusCode)
	require.Equal(t, contentType, response.Header.Get("Content-Type"))
	require.Equal(t, body, w.Body.String())
}
