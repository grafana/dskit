package memberlist

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestTCPTransport_SentAndReceivedBytesMetrics(t *testing.T) {
	setup := func(t *testing.T) (senderTransport, receiverTransport *TCPTransport, receiverAddr string, testData []byte) {
		logs := &concurrency.SyncBuffer{}
		logger := log.NewLogfmtLogger(logs)

		reg := prometheus.NewPedanticRegistry()

		// Set up the receiver transport
		receiverCfg := TCPTransportConfig{}
		flagext.DefaultValues(&receiverCfg)
		receiverCfg.BindAddrs = getLocalhostAddrs()
		receiverCfg.BindPort = 0 // Use random port
		receiverCfg.MetricsNamespace = "receiver"

		receiverTransport, err := NewTCPTransport(receiverCfg, logger, reg)
		require.NoError(t, err)

		// Get the advertised address
		receiverIP, receiverPort, err := receiverTransport.FinalAdvertiseAddr("", receiverTransport.GetAutoBindPort())
		require.NoError(t, err)
		receiverAddr = net.JoinHostPort(receiverIP.String(), fmt.Sprintf("%d", receiverPort))

		// Set up the sender transport
		senderCfg := TCPTransportConfig{}
		flagext.DefaultValues(&senderCfg)
		senderCfg.BindAddrs = getLocalhostAddrs()
		senderCfg.BindPort = 0 // Use random port
		senderCfg.MetricsNamespace = "sender"

		senderTransport, err = NewTCPTransport(senderCfg, logger, reg)
		require.NoError(t, err)

		// Prepare test data - use a size larger than flush threshold to test periodic flushing
		testData = make([]byte, 128*1024) // 128KB
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		return senderTransport, receiverTransport, receiverAddr, testData
	}

	t.Run("WriteTo() packet mode", func(t *testing.T) {
		senderTransport, receiverTransport, receiverAddr, testData := setup(t)

		// Send a packet from sender to receiver
		_, err := senderTransport.WriteTo(testData, receiverAddr)
		require.NoError(t, err)

		// Wait for the packet to be received
		select {
		case pkt := <-receiverTransport.PacketCh():
			require.Equal(t, testData, pkt.Buf)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for packet")
		}

		// Shutdown transports to ensure all connections are closed and metrics are flushed
		require.NoError(t, senderTransport.Shutdown())
		require.NoError(t, receiverTransport.Shutdown())

		// Ensure metrics are tracked.
		assert.GreaterOrEqual(t, testutil.ToFloat64(senderTransport.sentBytes), float64(len(testData)), "sender should have sent at least the test data size")
		assert.GreaterOrEqual(t, testutil.ToFloat64(receiverTransport.receivedBytes), float64(len(testData)), "receiver should have received at least the test data size")
	})

	t.Run("DialTimeout() stream mode", func(t *testing.T) {
		senderTransport, receiverTransport, receiverAddr, testData := setup(t)

		// Open a stream connection from sender to receiver
		conn, err := senderTransport.DialTimeout(receiverAddr, 5*time.Second)
		require.NoError(t, err)

		// Wait for the receiver to accept the stream connection
		var receiverConn net.Conn
		select {
		case receiverConn = <-receiverTransport.StreamCh():
			require.NotNil(t, receiverConn)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for stream connection")
		}

		// Write data from sender to receiver
		n, err := conn.Write(testData)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)

		// Read data on the receiver side
		receivedData := make([]byte, len(testData))
		_, err = io.ReadFull(receiverConn, receivedData)
		require.NoError(t, err)
		require.Equal(t, testData, receivedData)

		// Close connections
		require.NoError(t, conn.Close())
		require.NoError(t, receiverConn.Close())

		// Shutdown transports to ensure all connections are closed and metrics are flushed
		require.NoError(t, senderTransport.Shutdown())
		require.NoError(t, receiverTransport.Shutdown())

		// Ensure metrics are tracked.
		assert.GreaterOrEqual(t, testutil.ToFloat64(senderTransport.sentBytes), float64(len(testData)), "sender should have sent at least the test data size")
		assert.GreaterOrEqual(t, testutil.ToFloat64(receiverTransport.receivedBytes), float64(len(testData)), "receiver should have received at least the test data size")
	})
}
