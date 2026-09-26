// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/c9000f7becebd6cd6543b2f6dbcf825d3bfae49e/pkg/cortex/server_service.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-kit/log/level"

	"github.com/grafana/dskit/services"
)

// NewService manages the server's lifetime, waiting for dependent services before shutdown.
// Configure the server with DisableSignalHandling before constructing it. The callback
// is evaluated during shutdown and must not include the returned service itself.
// An unexpected return from Run fails the service.
func NewService(serv *Server, servicesToWaitFor func() []services.Service) services.Service {
	serverDone := make(chan error, 1)

	// BasicService can skip runFn on cancellation during startup, but still calls stoppingFn.
	startFn := func(context.Context) error {
		go func() {
			defer close(serverDone)
			serverDone <- serv.Run()
		}()
		return nil
	}

	runFn := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return nil
		case err := <-serverDone:
			if err != nil {
				return err
			}
			return fmt.Errorf("server stopped unexpectedly")
		}
	}

	stoppingFn := func(_ error) error {
		// Keep the server available until dependent services finish shutting down.
		var dependencies []services.Service
		if servicesToWaitFor != nil {
			dependencies = servicesToWaitFor()
		}
		for _, s := range dependencies {
			_ = s.AwaitTerminated(context.Background())
		}

		serv.Shutdown()

		// Shutdown closes the listeners but does not release the signal-handler goroutine.
		serv.Stop()

		<-serverDone
		level.Info(serv.Log).Log("msg", "server stopped")
		return nil
	}

	return services.NewBasicService(startFn, runFn, stoppingFn)
}

// DisableSignalHandling leaves shutdown under service lifecycle control.
// Call it before New when using NewService.
func DisableSignalHandling(config *Config) {
	config.SignalHandler = &ignoreSignalHandler{done: make(chan struct{})}
}

type ignoreSignalHandler struct {
	done chan struct{}
	once sync.Once
}

func (h *ignoreSignalHandler) Loop() {
	<-h.done
}

func (h *ignoreSignalHandler) Stop() {
	h.once.Do(func() { close(h.done) })
}
