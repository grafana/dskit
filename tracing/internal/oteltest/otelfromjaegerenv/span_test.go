// Package opentracingtest tests tracing with OTel installed.
package otelfromjaegerenv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/dskit/tracing"
)

var tracer = otel.Tracer("dskit.test")

func init() {
	// Install OTel tracing.
	_, err := tracing.NewOTelFromJaegerEnv("test")
	if err != nil {
		panic(err)
	}
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
