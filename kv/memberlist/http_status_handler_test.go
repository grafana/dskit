package memberlist

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/services"
)

func TestPage(t *testing.T) {
	// Setup codec
	c := dataCodec{}

	// Create and configure KVInitService
	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
	}
	cfg.Codecs = []codec.Codec{c}

	kvs := NewKVInitService(&cfg, log.NewNopLogger(), &staticDNSProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), kvs))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), kvs)
	})

	// Get KV client and populate data
	mkv, err := kvs.GetMemberlistKV()
	require.NoError(t, err)

	kv, err := NewClient(mkv, c)
	require.NoError(t, err)

	// Insert test data
	err = kv.CAS(context.Background(), "test-key", func(in interface{}) (out interface{}, retry bool, err error) {
		return &data{Members: map[string]member{"member1": {Timestamp: time.Now().Unix(), State: ACTIVE}}}, true, nil
	})
	require.NoError(t, err)

	// Test HTTP handler
	recorder := httptest.NewRecorder()
	kvs.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/memberlist", nil))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))

	// Verify Size column header
	assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
		"<th>", "Size", "</th>",
	}, `\s*`))), recorder.Body.String())

	// Verify size value appears in a table cell (non-zero)
	assert.Regexp(t, regexp.MustCompile(`<td>\d+</td>`), recorder.Body.String())

	// Verify Total row exists
	assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
		"<strong>", "Total:", "</strong>",
	}, `\s*`))), recorder.Body.String())
}
