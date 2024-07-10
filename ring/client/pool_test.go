package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/dskit/services"
)

type mockClient struct {
	happy  bool
	status grpc_health_v1.HealthCheckResponse_ServingStatus
}

func (i mockClient) Check(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	if !i.happy {
		return nil, fmt.Errorf("Fail")
	}
	return &grpc_health_v1.HealthCheckResponse{Status: i.status}, nil
}

func (i mockClient) Close() error {
	return nil
}

func (i mockClient) Watch(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (grpc_health_v1.Health_WatchClient, error) {
	return nil, status.Error(codes.Unimplemented, "Watching is not supported")
}

func TestHealthCheck(t *testing.T) {
	tcs := []struct {
		client   mockClient
		hasError bool
	}{
		{mockClient{happy: true, status: grpc_health_v1.HealthCheckResponse_UNKNOWN}, true},
		{mockClient{happy: true, status: grpc_health_v1.HealthCheckResponse_SERVING}, false},
		{mockClient{happy: true, status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}, true},
		{mockClient{happy: false, status: grpc_health_v1.HealthCheckResponse_UNKNOWN}, true},
		{mockClient{happy: false, status: grpc_health_v1.HealthCheckResponse_SERVING}, true},
		{mockClient{happy: false, status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}, true},
	}
	for _, tc := range tcs {
		err := healthCheck(context.Background(), tc.client, 50*time.Millisecond)
		hasError := err != nil
		if hasError != tc.hasError {
			t.Errorf("Expected error: %t, error: %v", tc.hasError, err)
		}
	}
}

func TestPoolCache(t *testing.T) {
	buildCount := 0
	factory := PoolAddrFunc(func(addr string) (PoolClient, error) {
		if addr == "bad" {
			return nil, fmt.Errorf("Fail")
		}
		buildCount++
		return mockClient{happy: true, status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
	})

	cfg := PoolConfig{
		HealthCheckTimeout: 50 * time.Millisecond,
		CheckInterval:      10 * time.Second,
	}

	pool := NewPool("test", cfg, nil, factory, nil, log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), pool))
	defer services.StopAndAwaitTerminated(context.Background(), pool) //nolint:errcheck

	_, err := pool.GetClientFor("1")
	require.NoError(t, err)
	if buildCount != 1 {
		t.Errorf("Did not create client")
	}

	_, err = pool.GetClientFor("1")
	require.NoError(t, err)
	if buildCount != 1 {
		t.Errorf("Created client that should have been cached")
	}

	_, err = pool.GetClientFor("2")
	require.NoError(t, err)
	if pool.Count() != 2 {
		t.Errorf("Expected Count() = 2, got %d", pool.Count())
	}

	pool.RemoveClientFor("1")
	if pool.Count() != 1 {
		t.Errorf("Expected Count() = 1, got %d", pool.Count())
	}

	_, err = pool.GetClientFor("1")
	require.NoError(t, err)
	if buildCount != 3 || pool.Count() != 2 {
		t.Errorf("Did not re-create client correctly")
	}

	_, err = pool.GetClientFor("bad")
	if err == nil {
		t.Errorf("Bad create should have thrown an error")
	}
	if pool.Count() != 2 {
		t.Errorf("Bad create should not have been added to cache")
	}

	addrs := pool.RegisteredAddresses()
	if len(addrs) != pool.Count() {
		t.Errorf("Lengths of registered addresses and cache.Count() do not match")
	}
}

func TestCleanUnhealthy(t *testing.T) {
	tcs := []struct {
		maxConcurrent int
	}{
		{maxConcurrent: 0}, // if not set, defaults to 16
		{maxConcurrent: 1},
	}
	for _, tc := range tcs {
		t.Run(fmt.Sprintf("max concurrent %d", tc.maxConcurrent), func(t *testing.T) {
			goodAddrs := []string{"good1", "good2"}
			badAddrs := []string{"bad1", "bad2"}
			clients := map[string]PoolClient{}
			for _, addr := range goodAddrs {
				clients[addr] = mockClient{happy: true, status: grpc_health_v1.HealthCheckResponse_SERVING}
			}
			for _, addr := range badAddrs {
				clients[addr] = mockClient{happy: false, status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}
			}

			cfg := PoolConfig{
				MaxConcurrentHealthChecks: tc.maxConcurrent,
				CheckInterval:             1 * time.Second,
				HealthCheckTimeout:        5 * time.Millisecond,
			}
			pool := NewPool("test", cfg, nil, nil, nil, log.NewNopLogger())
			pool.clients = clients
			pool.cleanUnhealthy()

			for _, addr := range badAddrs {
				if _, ok := pool.clients[addr]; ok {
					t.Errorf("Found bad client after clean: %s\n", addr)
				}
			}
			for _, addr := range goodAddrs {
				if _, ok := pool.clients[addr]; !ok {
					t.Errorf("Could not find good client after clean: %s\n", addr)
				}
			}
		})
	}
}

func TestRemoveClient(t *testing.T) {
	const (
		addr1 = "localhost:123"
		addr2 = "localhost:234"
	)

	assertNotEqual := func(t *testing.T, clientA, clientB PoolClient, msg string) {
		t.Helper()
		// Using golang equality. `assert.NotEqual` inspects the value of the interface implementation.
		if clientA == clientB {
			t.Error(msg)
		}
	}

	assertEqual := func(t *testing.T, clientA, clientB PoolClient, msg string) {
		t.Helper()
		// Using golang equality. `assert.Equal` inspects the value of the interface implementation.
		if clientA != clientB {
			t.Error(msg)
		}
	}

	clientHappy := true
	factory := PoolAddrFunc(func(string) (PoolClient, error) {
		return &mockClient{happy: clientHappy, status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
	})

	cfg := PoolConfig{
		CheckInterval:      time.Second,
		HealthCheckEnabled: false,
	}
	pool := NewPool("test", cfg, nil, factory, nil, log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), pool))
	defer services.StopAndAwaitTerminated(context.Background(), pool) //nolint:errcheck

	c1a, err := pool.GetClientFor(addr1)
	require.NoError(t, err)

	c2a, err := pool.GetClientFor(addr2)
	require.NoError(t, err)
	assertNotEqual(t, c1a, c2a, "clients for different addresses should be different")

	// Remove without providing address
	pool.RemoveClient(c1a, "")

	c1b, err := pool.GetClientFor(addr1)
	require.NoError(t, err)
	assertNotEqual(t, c1a, c1b, "c1a should have been removed and c1b should be a different instance")

	c2b, err := pool.GetClientFor(addr2)
	require.NoError(t, err)
	assertEqual(t, c2b, c2a, "c2b should be the same")

	// Remove with providing an address
	pool.RemoveClient(c1b, addr1)

	// Prepare for cleaning up unhealthy instances later
	clientHappy = false

	c1c, err := pool.GetClientFor(addr1)
	require.NoError(t, err)
	assertNotEqual(t, c1a, c1c, "c1a should have been removed and c1c should be a different instance")
	assertNotEqual(t, c1b, c1c, "c1b should have been removed and c1c should be a different instance")

	c2c, err := pool.GetClientFor(addr2)
	require.NoError(t, err)
	assertEqual(t, c2c, c2a, "c2c should be the same")

	// This should clean up c1c
	pool.cleanUnhealthy()
	// Removing c1c should be a noop
	pool.RemoveClient(c1c, addr1)

	c1d, err := pool.GetClientFor(addr1)
	require.NoError(t, err)
	assertNotEqual(t, c1a, c1d, "c1a should have been removed and c1d should be a different instance")
	assertNotEqual(t, c1b, c1d, "c1b should have been removed and c1d should be a different instance")

	c2d, err := pool.GetClientFor(addr2)
	require.NoError(t, err)
	assertEqual(t, c2d, c2a, "c2c should be the same")
}
