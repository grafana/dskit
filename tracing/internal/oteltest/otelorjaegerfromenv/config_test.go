package otelorjaegerfromenv

import (
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/tracing"
)

func TestTracingNewOTelOrJaegerFromEnvDoesNotFailWithNoEnv(t *testing.T) {
	// Make sure no tracing env is set for the test.
	defer unsetAndRestoreDeferred(
		"JAEGER_AGENT_HOST",
		"JAEGER_ENDPOINT",
		"JAEGER_SAMPLER_MANAGER_HOST_PORT",
		"OTEL_TRACES_EXPORTER",
	)()

	closer, err := tracing.NewOTelOrJaegerFromEnv("test-service", log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := closer.Close()
		require.NoError(t, err)
	})
}

func unsetAndRestoreDeferred(vars ...string) func() {
	originalValues := make(map[string]string)
	for _, k := range vars {
		originalValues[k] = os.Getenv(k)
		os.Unsetenv(k)
	}

	return func() {
		for _, v := range vars {
			if originalValue, exists := originalValues[v]; exists {
				if originalValue == "" {
					os.Unsetenv(v)
				} else {
					os.Setenv(v, originalValue)
				}
			} else {
				os.Unsetenv(v)
			}
		}
	}
}
