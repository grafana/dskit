package grpcutil

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/dskit/services"
)

func TestHealthCheck_Check_ServiceManager(t *testing.T) {
	tests := map[string]struct {
		states   []services.State
		expected grpc_health_v1.HealthCheckResponse_ServingStatus
	}{
		"all services are new": {
			states:   []services.State{services.New, services.New},
			expected: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
		"all services are starting": {
			states:   []services.State{services.Starting, services.Starting},
			expected: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
		"some services are starting and some running": {
			states:   []services.State{services.Starting, services.Running},
			expected: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
		"all services are running": {
			states:   []services.State{services.Running, services.Running},
			expected: grpc_health_v1.HealthCheckResponse_SERVING,
		},
		"some services are stopping": {
			states:   []services.State{services.Running, services.Stopping},
			expected: grpc_health_v1.HealthCheckResponse_SERVING,
		},
		"some services are terminated while others running": {
			states:   []services.State{services.Running, services.Terminated},
			expected: grpc_health_v1.HealthCheckResponse_SERVING,
		},
		"all services are stopping": {
			states:   []services.State{services.Stopping, services.Stopping},
			expected: grpc_health_v1.HealthCheckResponse_SERVING,
		},
		"some services are terminated while others stopping": {
			states:   []services.State{services.Stopping, services.Terminated},
			expected: grpc_health_v1.HealthCheckResponse_SERVING,
		},
		"a service has failed while others are running": {
			states:   []services.State{services.Running, services.Failed},
			expected: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
		"all services are terminated": {
			states:   []services.State{services.Terminated, services.Terminated},
			expected: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var svcs []services.Service
			for range testData.states {
				svcs = append(svcs, &mockService{})
			}

			ctx := context.Background()
			req := &grpc_health_v1.HealthCheckRequest{}
			sm, err := services.NewManager(svcs...)
			require.NoError(t, err)

			// Switch the state of each mocked services.
			for i, s := range svcs {
				s.(*mockService).switchState(testData.states[i])
			}

			h := NewHealthCheckFrom(WithManager(sm))
			res, err := h.Check(ctx, req)

			require.NoError(t, err)
			require.Equal(t, testData.expected, res.Status)
		})
	}
}

func TestHealthCheck_Check_ShutdownRequested(t *testing.T) {
	tests := map[string]struct {
		requested *atomic.Bool
		expected  grpc_health_v1.HealthCheckResponse_ServingStatus
	}{
		"shutdown not requested": {
			requested: atomic.NewBool(false),
			expected:  grpc_health_v1.HealthCheckResponse_SERVING,
		},
		"shutdown is requested": {
			requested: atomic.NewBool(true),
			expected:  grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			req := &grpc_health_v1.HealthCheckRequest{}

			h := NewHealthCheckFrom(WithShutdownRequested(testData.requested))
			res, err := h.Check(ctx, req)

			require.NoError(t, err)
			require.Equal(t, testData.expected, res.Status)
		})
	}
}

type mockService struct {
	services.Service
	state     services.State
	listeners []services.Listener
}

func (s *mockService) switchState(desiredState services.State) {
	// Simulate all the states between the current state and the desired one.
	orderedStates := []services.State{services.New, services.Starting, services.Running, services.Failed, services.Stopping, services.Terminated}
	simulationStarted := false

	for _, orderedState := range orderedStates {
		// Skip until we reach the current state.
		if !simulationStarted && orderedState != s.state {
			continue
		}

		// Start the simulation once we reach the current state.
		if orderedState == s.state {
			simulationStarted = true
			continue
		}

		// Skip the failed state, unless it's the desired one.
		if orderedState == services.Failed && desiredState != services.Failed {
			continue
		}

		s.state = orderedState

		// Synchronously call listeners to avoid flaky tests.
		for _, listener := range s.listeners {
			switch orderedState {
			case services.Starting:
				listener.Starting()
			case services.Running:
				listener.Running()
			case services.Stopping:
				listener.Stopping(services.Running)
			case services.Failed:
				listener.Failed(services.Running, errors.New("mocked error"))
			case services.Terminated:
				listener.Terminated(services.Stopping)
			}
		}

		if orderedState == desiredState {
			break
		}
	}
}

func (s *mockService) State() services.State {
	return s.state
}

func (s *mockService) AddListener(listener services.Listener) {
	s.listeners = append(s.listeners, listener)
}

func (s *mockService) StartAsync(_ context.Context) error      { return nil }
func (s *mockService) AwaitRunning(_ context.Context) error    { return nil }
func (s *mockService) StopAsync()                              {}
func (s *mockService) AwaitTerminated(_ context.Context) error { return nil }
func (s *mockService) FailureCase() error                      { return nil }
