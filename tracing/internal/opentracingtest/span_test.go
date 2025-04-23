// Package opentracingtest tests tracing with a global opentracing.TracerProvider registered.
package opentracingtest

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"

	"github.com/grafana/dskit/tracing"
)

func init() {
	// Install opentracing.
	_, err := tracing.NewFromEnv("test")
	if err != nil {
		panic(err)
	}
}

func TestStartSpanFromContext(t *testing.T) {
	t.Run("without parent", func(t *testing.T) {
		// Start a new span from the context
		newSpan, ctx := tracing.StartSpanFromContext(context.Background(), "test-span")
		defer newSpan.Finish()

		spanFromContext := opentracing.SpanFromContext(ctx)
		_, ok := spanFromContext.Context().(jaeger.SpanContext)
		require.True(t, ok, "Expected span context to be of type jaeger.SpanContext")
	})

	t.Run("with parent", func(t *testing.T) {
		// Create a new context with a span
		ctx := context.Background()
		tracer := opentracing.GlobalTracer()
		span := tracer.StartSpan("test-span")
		defer span.Finish()

		jaegerSpanContext, ok := span.Context().(jaeger.SpanContext)
		require.True(t, ok, "Expected span context to be of type jaeger.SpanContext")
		require.NotZero(t, jaegerSpanContext.TraceID(), "Expected non-zero trace ID")

		// Set the span in the context
		ctx = opentracing.ContextWithSpan(ctx, span)

		// Start a new span from the context
		newSpan, ctx := tracing.StartSpanFromContext(ctx, "child-span")
		defer newSpan.Finish()

		spanFromContext := opentracing.SpanFromContext(ctx)
		jaegerSpanFromContextContext, ok := spanFromContext.Context().(jaeger.SpanContext)
		require.True(t, ok, "Expected span context to be of type jaeger.SpanContext")
		require.Equal(t, jaegerSpanContext.TraceID().String(), jaegerSpanFromContextContext.TraceID().String(), "Expected trace ID to match parent span")
	})
}
