package memberlist

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/concurrency"
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
			cfg.BindAddrs = []string{"127.0.0.1"}
			cfg.BindPort = 0
			if testData.setup != nil {
				testData.setup(t, &cfg)
			}

			transport, err := NewTCPTransport(cfg, logger, nil)
			require.NoError(t, err)

			_, err = transport.WriteTo([]byte("test"), testData.remoteAddr)
			require.NoError(t, err)

			if testData.expectedLogs != "" {
				assert.Contains(t, logs.String(), testData.expectedLogs)
			}
			if testData.unexpectedLogs != "" {
				assert.NotContains(t, logs.String(), testData.unexpectedLogs)
			}
		})
	}
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
