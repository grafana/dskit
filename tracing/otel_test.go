package tracing

import (
	"context"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestExtractSampledTraceID(t *testing.T) {
	cases := []struct {
		desc  string
		ctx   func(*testing.T) (context.Context, func())
		empty bool
	}{
		{
			desc: "OpenTelemetry",
			ctx:  getContextWithOpenTelemetry,
		},
		{
			desc: "No tracer",
			ctx: func(_ *testing.T) (context.Context, func()) {
				return context.Background(), func() {}
			},
			empty: true,
		},
		{
			desc:  "OpenTelemetry with noop",
			ctx:   getContextWithOpenTelemetryNoop,
			empty: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, closer := tc.ctx(t)
			defer closer()
			sampledTraceID, sampled := ExtractSampledTraceID(ctx)
			traceID, ok := ExtractTraceID(ctx)

			assert.Equal(t, sampledTraceID, traceID, "Expected sampledTraceID to equal traceID")
			if tc.empty {
				assert.Empty(t, traceID, "Expected traceID to be empty")
				assert.False(t, sampled, "Expected sampled to be false")
				assert.False(t, ok, "Expected ok to be false")
			} else {
				assert.NotEmpty(t, traceID, "Expected traceID to be non-empty")
				assert.True(t, sampled, "Expected sampled to be true")
				assert.True(t, ok, "Expected ok to be true")
			}
		})
	}
}

func getContextWithOpenTelemetry(_ *testing.T) (context.Context, func()) {
	originTracerProvider := otel.GetTracerProvider()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	tr := tp.Tracer("test")
	ctx, sp := tr.Start(context.Background(), "test")
	return ctx, func() {
		sp.End()
		otel.SetTracerProvider(originTracerProvider)
	}
}

func getContextWithOpenTelemetryNoop(t *testing.T) (context.Context, func()) {
	ctx, sp := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	// sanity check
	require.False(t, sp.SpanContext().TraceID().IsValid())
	return ctx, func() {
		sp.End()
	}
}

func TestNewResource(t *testing.T) {
	res, err := NewResource("test-service", []attribute.KeyValue{
		attribute.String("test.key", "test.value"),
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Contains(t, res.Attributes(), attribute.String("service.name", "test-service"))
	require.Contains(t, res.Attributes(), attribute.String("test.key", "test.value"))
}

func TestNewOTelFromEnv(t *testing.T) {
	t.Run("with none exporter", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_TRACES_EXPORTER")()

		// Set to none to avoid actual export
		os.Setenv("OTEL_TRACES_EXPORTER", "none")

		closer, err := NewOTelFromEnv("test-service", log.NewNopLogger())
		require.NoError(t, err)
		require.NotNil(t, closer)
		defer closer.Close()

		// Verify tracer provider is configured
		tp := otel.GetTracerProvider()
		require.NotNil(t, tp)

		// Verify we can create spans
		tr := tp.Tracer("test")
		_, span := tr.Start(context.Background(), "test-operation")
		defer span.End()

		require.True(t, span.SpanContext().IsValid())
	})

	t.Run("with custom resource attributes", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_TRACES_EXPORTER")()

		os.Setenv("OTEL_TRACES_EXPORTER", "none")

		closer, err := NewOTelFromEnv("test-service", log.NewNopLogger(),
			WithResourceAttributes(
				attribute.String("service.version", "1.0.0"),
				attribute.String("deployment.environment", "test"),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, closer)
		defer closer.Close()
	})

	t.Run("with pyroscope disabled", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_TRACES_EXPORTER")()

		os.Setenv("OTEL_TRACES_EXPORTER", "none")

		closer, err := NewOTelFromEnv("test-service", log.NewNopLogger(), WithPyroscopeDisabled())
		require.NoError(t, err)
		require.NotNil(t, closer)
		defer closer.Close()
	})
}

func TestMaybeJaegerRemoteSamplerFromEnv(t *testing.T) {
	t.Run("no sampler configured", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG")()

		os.Unsetenv("OTEL_TRACES_SAMPLER")
		os.Unsetenv("OTEL_TRACES_SAMPLER_ARG")

		sampler, ok, err := MaybeJaegerRemoteSamplerFromEnv("test-service")
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, sampler)
	})

	t.Run("jaeger_remote sampler with valid args", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG")()

		os.Setenv("OTEL_TRACES_SAMPLER", "jaeger_remote")
		os.Setenv("OTEL_TRACES_SAMPLER_ARG", "endpoint=http://localhost:14250,pollingIntervalMs=5000,initialSamplingRate=0.25")

		sampler, ok, err := MaybeJaegerRemoteSamplerFromEnv("test-service")
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, sampler)

		// Clean up sampler to avoid goroutine leak. Don't check the type, it should always be closeable.
		sampler.(interface{ Close() }).Close()
	})

	t.Run("parentbased_jaeger_remote sampler", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG")()

		os.Setenv("OTEL_TRACES_SAMPLER", "parentbased_jaeger_remote")
		os.Setenv("OTEL_TRACES_SAMPLER_ARG", "endpoint=http://localhost:14250,pollingIntervalMs=5000")

		sampler, ok, err := MaybeJaegerRemoteSamplerFromEnv("test-service")
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, sampler)

		// Clean up sampler to avoid goroutine leak. Don't check the type, it should always be closeable.
		sampler.(interface{ Close() }).Close()
	})

	t.Run("missing sampler args", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG")()

		os.Setenv("OTEL_TRACES_SAMPLER", "jaeger_remote")
		os.Unsetenv("OTEL_TRACES_SAMPLER_ARG")

		sampler, ok, err := MaybeJaegerRemoteSamplerFromEnv("test-service")
		require.Error(t, err)
		require.False(t, ok)
		require.Nil(t, sampler)
		require.Contains(t, err.Error(), "OTEL_TRACES_SAMPLER_ARG is not set")
	})

	t.Run("missing endpoint in args", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG")()

		os.Setenv("OTEL_TRACES_SAMPLER", "jaeger_remote")
		os.Setenv("OTEL_TRACES_SAMPLER_ARG", "pollingIntervalMs=5000,initialSamplingRate=0.25")

		sampler, ok, err := MaybeJaegerRemoteSamplerFromEnv("test-service")
		require.Error(t, err)
		require.False(t, ok)
		require.Nil(t, sampler)
		require.Contains(t, err.Error(), "endpoint is not set")
	})

	t.Run("invalid polling interval", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG")()

		os.Setenv("OTEL_TRACES_SAMPLER", "jaeger_remote")
		os.Setenv("OTEL_TRACES_SAMPLER_ARG", "endpoint=http://localhost:14250,pollingIntervalMs=invalid")

		sampler, ok, err := MaybeJaegerRemoteSamplerFromEnv("test-service")
		require.Error(t, err)
		require.False(t, ok)
		require.Nil(t, sampler)
		require.Contains(t, err.Error(), "invalid pollingIntervalMs value")
	})

	t.Run("invalid initial sampling rate", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG")()

		os.Setenv("OTEL_TRACES_SAMPLER", "jaeger_remote")
		os.Setenv("OTEL_TRACES_SAMPLER_ARG", "endpoint=http://localhost:14250,initialSamplingRate=2.0")

		sampler, ok, err := MaybeJaegerRemoteSamplerFromEnv("test-service")
		require.Error(t, err)
		require.False(t, ok)
		require.Nil(t, sampler)
		require.Contains(t, err.Error(), "initialSamplingRate value set in OTEL_TRACES_SAMPLER_ARG must be between 0 and 1")
	})

	t.Run("other sampler type", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG")()

		os.Setenv("OTEL_TRACES_SAMPLER", "always_on")

		sampler, ok, err := MaybeJaegerRemoteSamplerFromEnv("test-service")
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, sampler)
	})
}

func TestOTelPropagatorsFromEnv(t *testing.T) {
	t.Run("default propagators when env not set", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_PROPAGATORS")()

		os.Unsetenv("OTEL_PROPAGATORS")

		propagators := OTelPropagatorsFromEnv()
		require.Len(t, propagators, 3)
	})

	t.Run("custom propagators from env", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_PROPAGATORS")()

		os.Setenv("OTEL_PROPAGATORS", "tracecontext,baggage")

		propagators := OTelPropagatorsFromEnv()
		require.Len(t, propagators, 2)
	})

	t.Run("none propagator", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_PROPAGATORS")()

		os.Setenv("OTEL_PROPAGATORS", "none")

		propagators := OTelPropagatorsFromEnv()
		require.Nil(t, propagators)
	})

	t.Run("jaeger propagator", func(t *testing.T) {
		defer saveEnvAndRestoreDeferred("OTEL_PROPAGATORS")()

		os.Setenv("OTEL_PROPAGATORS", "jaeger")

		propagators := OTelPropagatorsFromEnv()
		require.Len(t, propagators, 1)
	})
}

func saveEnvAndRestoreDeferred(vars ...string) func() {
	originalValues := make(map[string]string)
	for _, v := range vars {
		originalValues[v] = os.Getenv(v)
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
