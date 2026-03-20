package runtimeconfig

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsURL(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"http://example.com/config.yaml", true},
		{"https://example.com/config.yaml", true},
		{"HTTP://EXAMPLE.COM/config.yaml", false}, // case-sensitive
		{"/etc/config.yaml", false},
		{"config.yaml", false},
		{"", false},
		{"ftp://example.com/config.yaml", false},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			assert.Equal(t, tt.want, isURL(tt.path))
		})
	}
}

func newTestHTTPProvider(t *testing.T, url string, client *http.Client) (*httpProvider, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	dur := newHTTPRequestDuration(reg)
	return newHTTPProvider(url, client, dur), reg
}

func TestHTTPProvider_Success(t *testing.T) {
	body := "overrides:\n  user1:\n    limit1: 100\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	p, _ := newTestHTTPProvider(t, srv.URL+"/config.yaml", &http.Client{})

	data, err := p.Read(context.Background())
	require.NoError(t, err)
	assert.Equal(t, body, string(data))
	assert.Equal(t, srv.URL+"/config.yaml", p.Name())

	assert.Equal(t, 1, testutil.CollectAndCount(p.requestDuration))
}

func TestHTTPProvider_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p, _ := newTestHTTPProvider(t, srv.URL+"/config.yaml", &http.Client{})

	_, err := p.Read(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 500")

	var httpErr *httpError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, 500, httpErr.statusCode)

	assert.Equal(t, 1, testutil.CollectAndCount(p.requestDuration))
}

func TestHTTPProvider_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	p, _ := newTestHTTPProvider(t, srv.URL+"/config.yaml", &http.Client{})

	_, err := p.Read(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 403")

	var httpErr *httpError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, 403, httpErr.statusCode)
}

func TestHTTPProvider_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(10 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	p, _ := newTestHTTPProvider(t, srv.URL+"/config.yaml", &http.Client{Timeout: 50 * time.Millisecond})

	_, err := p.Read(context.Background())
	require.Error(t, err)

	assert.Equal(t, 1, testutil.CollectAndCount(p.requestDuration))
}

func TestHTTPProvider_ContextCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(10 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	p, _ := newTestHTTPProvider(t, srv.URL+"/config.yaml", &http.Client{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := p.Read(ctx)
	require.Error(t, err)
}
