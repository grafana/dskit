// Package otelfromenv tests tracing with OTel auto exporter installed.
package otelfromenv

import (
	"context"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/dskit/tracing"
)

var tracer = otel.Tracer("dskit.test")

func init() {
	// Set OTEL_TRACES_EXPORTER to none to avoid actual export during tests
	os.Setenv("OTEL_TRACES_EXPORTER", "none")

	// Install OTel tracing.
	closer, err := tracing.NewOTelFromEnv("test", log.NewNopLogger())
	if err != nil {
		panic(err)
	}
	// Note: In real usage, you'd defer closer.Close(), but for init() we leave it open
	_ = closer
}

func TestStartSpanFromContext(t *testing.T) {
	t.Run("without parent", func(t *testing.T) {
		// Start a new span from the context
		newSpan, ctx := tracing.StartSpanFromContext(context.Background(), "test-span")
		defer newSpan.Finish()

		spanFromContext := trace.SpanFromContext(ctx)
		require.True(t, spanFromContext.SpanContext().IsValid())
	})

	t.Run("with parent", func(t *testing.T) {
		// Create a new context with a span
		ctx, span := tracer.Start(context.Background(), "test-span")
		defer span.End()
		require.NotZero(t, span.SpanContext().TraceID())

		// Start a new span from the context
		newSpan, ctx := tracing.StartSpanFromContext(ctx, "child-span")
		defer newSpan.Finish()

		spanFromContext := trace.SpanFromContext(ctx)
		require.Equal(t, span.SpanContext().TraceID().String(), spanFromContext.SpanContext().TraceID().String(), "Expected trace ID to match parent span")
	})
}

func TestOTelFromEnvInitialization(t *testing.T) {
	t.Run("can create tracer provider", func(t *testing.T) {
		// Save original exporter setting
		originalExporter := os.Getenv("OTEL_TRACES_EXPORTER")
		defer os.Setenv("OTEL_TRACES_EXPORTER", originalExporter)

		// Set to none to avoid actual export
		os.Setenv("OTEL_TRACES_EXPORTER", "none")

		closer, err := tracing.NewOTelFromEnv("test-service", log.NewNopLogger())
		require.NoError(t, err)
		require.NotNil(t, closer)
		defer closer.Close()

		// Verify tracer provider is set
		tp := otel.GetTracerProvider()
		require.NotNil(t, tp)

		// Verify we can create spans
		tr := tp.Tracer("test")
		_, span := tr.Start(context.Background(), "test-operation")
		defer span.End()

		require.True(t, span.SpanContext().IsValid())
		require.True(t, span.SpanContext().TraceID().IsValid())
		require.True(t, span.SpanContext().SpanID().IsValid())
	})
}
