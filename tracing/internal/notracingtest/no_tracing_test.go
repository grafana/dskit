package notracingtest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/grafana/dskit/tracing"
)

func TestNoTracingConfigured(t *testing.T) {
	// Make sure no tracing env is set for the test.
	defer unsetAndRestoreDeferred(
		"JAEGER_AGENT_HOST",
		"JAEGER_ENDPOINT",
		"JAEGER_SAMPLER_MANAGER_HOST_PORT",
		"OTEL_TRACES_EXPORTER",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
	)()
	logger := make(mockLogger, 100)

	t.Run("tracing.NewOTelOrJaegerFromEnv", func(t *testing.T) {
		closer, err := tracing.NewOTelOrJaegerFromEnv("test", logger)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, closer.Close()) })

		ensureTracingDoesNotLogErrors(t, logger)
	})
	t.Run("tracing.NewOTelFromEnv", func(t *testing.T) {
		closer, err := tracing.NewOTelFromEnv("test", logger)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, closer.Close()) })

		ensureTracingDoesNotLogErrors(t, logger)
	})
}

func ensureTracingDoesNotLogErrors(t *testing.T, logger mockLogger) {
	tracer := otel.Tracer("test")
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				// Generate A LOT of spans, so exporter will be forced to export them if configured.
				_, sp := tracer.Start(context.Background(), "test-operation")
				sp.End()
			}
		}
	}()

	// There's no way to figure out whether OTel tracer provider is configured or not, so we'll just observe the logs.
	timeout := time.After(time.Second)
	select {
	case log := <-logger:
		// If we let opentracing configure itself, it will log about the configuration, which isn't a big deal,
		// but then for each span batch exported it will complain about localhost:4318 not accepting traces.
		t.Errorf("Unexpected log: %v", log)
		// Read more logs, if any.
	logs:
		for {
			select {
			case log = <-logger:
				t.Errorf("Unexpected log: %v", log)
			case <-timeout:
				break logs
			}
		}
	case <-timeout:
		// No logs are expected.
	}
}

func unsetAndRestoreDeferred(vars ...string) func() {
	originalValues := make(map[string]string)
	for _, k := range vars {
		originalValues[k] = os.Getenv(k)
		os.Unsetenv(k)
	}

	return func() {
		for _, v := range vars {
			if originalValue := originalValues[v]; originalValue != "" {
				os.Setenv(v, originalValue)
			} else {
				os.Unsetenv(v)
			}
		}
	}
}

type mockLogger chan []any

func (m mockLogger) Log(keyvals ...any) error {
	m <- keyvals
	return nil
}
