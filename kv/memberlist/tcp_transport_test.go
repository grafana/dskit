package memberlist

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/flagext"
)

func TestTCPTransport_WriteTo_ShouldNotLogAsWarningExpectedFailures(t *testing.T) {
	tests := map[string]struct {
		setup          func(t *testing.T, cfg *TCPTransportConfig)
		remoteAddr     string
		expectedLogs   string
		unexpectedLogs string
	}{
		"should not log 'connection refused' by default": {
			remoteAddr:     "127.0.0.1:12345",
			unexpectedLogs: "connection refused",
		},
		"should log 'connection refused' if debug log level is enabled": {
			setup: func(_ *testing.T, cfg *TCPTransportConfig) {
				cfg.TransportDebug = true
			},
			remoteAddr:   "127.0.0.1:12345",
			expectedLogs: "connection refused",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			logs := &concurrency.SyncBuffer{}
			logger := log.NewLogfmtLogger(logs)

			cfg := TCPTransportConfig{}
			flagext.DefaultValues(&cfg)
			cfg.BindAddrs = getLocalhostAddrs()
			cfg.BindPort = 0
			if testData.setup != nil {
				testData.setup(t, &cfg)
			}

			transport, err := NewTCPTransport(cfg, logger, nil)
			require.NoError(t, err)

			_, err = transport.WriteTo([]byte("test"), testData.remoteAddr)
			require.NoError(t, err)

			require.NoError(t, transport.Shutdown())

			if testData.expectedLogs != "" {
				assert.Contains(t, logs.String(), testData.expectedLogs)
			}
			if testData.unexpectedLogs != "" {
				assert.NotContains(t, logs.String(), testData.unexpectedLogs)
			}
		})
	}
}

type timeoutReader struct{}

func (f *timeoutReader) ReadSecret(_ string) ([]byte, error) {
	time.Sleep(1 * time.Second)
	return nil, nil
}

func TestTCPTransportWriteToUnreachableAddr(t *testing.T) {
	writeCt := 50

	// Listen for TCP connections on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	cfg := TCPTransportConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BindAddrs = getLocalhostAddrs()
	cfg.MaxConcurrentWrites = writeCt
	cfg.PacketDialTimeout = 500 * time.Millisecond
	transport, err := NewTCPTransport(cfg, logger, nil)
	require.NoError(t, err)

	// Configure TLS only for writes. The dialing should timeout (because of the timeoutReader)
	transport.cfg.TLSEnabled = true
	transport.cfg.TLS = tls.ClientConfig{
		Reader:   &timeoutReader{},
		CertPath: "fake",
		KeyPath:  "fake",
		CAPath:   "fake",
	}

	timeStart := time.Now()

	for i := 0; i < writeCt; i++ {
		_, err = transport.WriteTo([]byte("test"), listener.Addr().String())
		require.NoError(t, err)
	}

	require.NoError(t, transport.Shutdown())

	gotErrorCt := strings.Count(logs.String(), "context deadline exceeded")
	assert.Equal(t, writeCt, gotErrorCt, "expected %d errors, got %d", writeCt, gotErrorCt)
	assert.GreaterOrEqual(t, time.Since(timeStart), 500*time.Millisecond, "expected to take at least 500ms (timeout duration)")
	assert.LessOrEqual(t, time.Since(timeStart), 2*time.Second, "expected to take less than 2s (timeout + a good margin), writing to unreachable addresses should not block")
}

func TestTCPTransportWriterAcquireTimeout(t *testing.T) {
	// Listen for TCP connections on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	cfg := TCPTransportConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BindAddrs = getLocalhostAddrs()
	cfg.MaxConcurrentWrites = 1
	cfg.AcquireWriterTimeout = 1 * time.Millisecond // very short timeout
	transport, err := NewTCPTransport(cfg, logger, nil)
	require.NoError(t, err)

	writeCt := 100
	var reqWg sync.WaitGroup
	for i := 0; i < writeCt; i++ {
		reqWg.Add(1)
		go func() {
			defer reqWg.Done()
			transport.WriteTo([]byte("test"), listener.Addr().String()) // nolint:errcheck
		}()
	}
	reqWg.Wait()

	require.NoError(t, transport.Shutdown())
	gotErrorCt := strings.Count(logs.String(), "WriteTo failed to acquire a writer. Dropping message")
	assert.Less(t, gotErrorCt, writeCt, "expected to have less errors (%d) than total writes (%d). Some writes should pass.", gotErrorCt, writeCt)
	assert.NotZero(t, gotErrorCt, "expected errors, got none")
}

func TestFinalAdvertiseAddr(t *testing.T) {
	tests := map[string]struct {
		advertiseAddr string
		bindAddrs     []string
		bindPort      int
	}{
		"should not fail with local address specified": {
			advertiseAddr: "127.0.0.1",
			bindAddrs:     []string{"127.0.0.1"},
			bindPort:      0,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			logs := &concurrency.SyncBuffer{}
			logger := log.NewLogfmtLogger(logs)

			cfg := TCPTransportConfig{}
			flagext.DefaultValues(&cfg)
			cfg.BindAddrs = testData.bindAddrs
			cfg.BindPort = testData.bindPort

			transport, err := NewTCPTransport(cfg, logger, prometheus.NewPedanticRegistry())
			require.NoError(t, err)

			ip, port, err := transport.FinalAdvertiseAddr(testData.advertiseAddr, testData.bindPort)
			require.NoError(t, err)
			require.Equal(t, testData.advertiseAddr, ip.String())
			require.Equal(t, testData.bindPort, port)

		})
	}
}

func TestNonIPsAreRejected(t *testing.T) {
	cfg := TCPTransportConfig{BindAddrs: flagext.StringSlice{"localhost"}}
	_, err := NewTCPTransport(cfg, nil, nil)
	require.EqualError(t, err, `could not parse bind addr "localhost" as IP address`)
}
