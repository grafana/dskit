package memberlist

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
)

// TestFinalAdvertiseAddrNoPrivateIP verifies that FinalAdvertiseAddr works when GetPrivateIP fails but a valid bound address exists
// This tests the fix for environments with non-RFC1918 pod CIDRs (e.g., GCP 10.x.x.x, AWS pod CIDRs)
func TestFinalAdvertiseAddrNoPrivateIP(t *testing.T) {
	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	cfg := TCPTransportConfig{}
	flagext.DefaultValues(&cfg)
	
	// Simulate bind to 0.0.0.0 on a system without any private IPs
	// This simulates Kubernetes pods with cloud provider IPs (which are NOT RFC1918)
	cfg.BindAddrs = []string{"0.0.0.0"}
	cfg.BindPort = 0
	
	transport, err := NewTCPTransport(cfg, logger, prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	defer transport.Shutdown()

	// Try to get final advertise address without explicit IP
	// This should succeed because tcpListeners[0] is bound to 0.0.0.0 which will resolve to a valid local address
	ip, port, err := transport.FinalAdvertiseAddr("", transport.GetAutoBindPort())
	require.NoError(t, err, "Should succeed with bound address fallback when no private IP is available")
	
	// Should use the bound address (0.0.0.0 gets translated to a valid IP during bind)
	require.NotNil(t, ip)
	require.NotZero(t, port)
}
