package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

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
