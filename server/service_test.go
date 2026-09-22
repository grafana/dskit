package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/grafana/dskit/services"
)

func TestNewServiceShutdownOrder(t *testing.T) {
	ignoreExisting := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, ignoreExisting) })
	serv := newLifecycleTestServer(t)
	dependency := services.NewIdleService(nil, nil)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), dependency))
	t.Cleanup(dependency.StopAsync)
	waiting := make(chan struct{})
	svc := NewService(serv, func() []services.Service {
		close(waiting)
		return []services.Service{dependency}
	})
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), svc))
	svc.StopAsync()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("server service did not begin stopping")
	}
	require.Equal(t, services.Stopping, svc.State())
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+serv.HTTPListenAddr().String()+"/ready", nil)
	require.NoError(t, err)
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, services.StopAndAwaitTerminated(t.Context(), dependency))
	require.NoError(t, svc.AwaitTerminated(t.Context()))
	require.Equal(t, services.Terminated, svc.State())
}

func TestNewServiceStopsWithoutDependencies(t *testing.T) {
	ignoreExisting := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, ignoreExisting) })
	serv := newLifecycleTestServer(t)
	svc := NewService(serv, nil)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), svc))
	require.NoError(t, services.StopAndAwaitTerminated(t.Context(), svc))
}

func TestNewServiceUnexpectedExit(t *testing.T) {
	for _, failure := range []bool{false, true} {
		name := "clean exit"
		if failure {
			name = "listener failure"
		}
		t.Run(name, func(t *testing.T) {
			ignoreExisting := goleak.IgnoreCurrent()
			t.Cleanup(func() { goleak.VerifyNone(t, ignoreExisting) })
			serv := newLifecycleTestServer(t)
			want := errors.New("listener failed")
			if failure {
				serv.httpListener = &failingLifecycleListener{Listener: serv.httpListener, err: want}
			}
			svc := NewService(serv, nil)
			require.NoError(t, svc.StartAsync(t.Context()))
			if !failure {
				serv.Stop()
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := svc.AwaitTerminated(ctx)
			require.Equal(t, services.Failed, svc.State())
			if failure {
				require.ErrorIs(t, err, want)
				require.ErrorIs(t, svc.FailureCase(), want)
			} else {
				require.EqualError(t, svc.FailureCase(), "server stopped unexpectedly")
				require.ErrorContains(t, err, "server stopped unexpectedly")
			}
		})
	}
}

type failingLifecycleListener struct {
	net.Listener
	err error
}

func (l *failingLifecycleListener) Accept() (net.Conn, error) { return nil, l.err }

func TestDisableSignalHandling(t *testing.T) {
	var cfg Config
	DisableSignalHandling(&cfg)
	done := make(chan struct{})
	go func() { cfg.SignalHandler.Loop(); close(done) }()
	cfg.SignalHandler.Stop()
	cfg.SignalHandler.Stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("signal handler did not stop")
	}
}

func newLifecycleTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := Config{
		HTTPListenAddress:             "127.0.0.1",
		GRPCListenAddress:             "127.0.0.1",
		ServerGracefulShutdownTimeout: time.Second,
		Registerer:                    prometheus.NewRegistry(),
		Log:                           log.NewNopLogger(),
	}
	DisableSignalHandling(&cfg)
	serv, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { serv.Shutdown(); serv.Stop() })
	serv.HTTP.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return serv
}

func TestNewServiceCanceledBeforeStart(t *testing.T) {
	ignoreExisting := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, ignoreExisting) })
	serv := newLifecycleTestServer(t)
	svc := NewService(serv, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, svc.StartAsync(ctx))
	stoppedCtx, stoppedCancel := context.WithTimeout(t.Context(), time.Second)
	defer stoppedCancel()
	require.NoError(t, svc.AwaitTerminated(stoppedCtx))
	require.Equal(t, services.Terminated, svc.State())
}
