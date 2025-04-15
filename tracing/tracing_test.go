// SPDX-License-Identifier: Apache-2.0

package tracing

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	jaeger "github.com/uber/jaeger-client-go"
	"go.opentelemetry.io/otel/trace"
)

const expectedTraceID = "00000000000000010000000000000002"

func TestExtractTraceSpanID_Opentracing(t *testing.T) {
	spanCtx := jaeger.NewSpanContext(jaeger.TraceID{High: 1, Low: 2}, jaeger.SpanID(3), 0, true, nil)
	tracer, closer := jaeger.NewTracer("test", jaeger.NewConstSampler(true), jaeger.NewNullReporter())
	defer closer.Close()
	span := tracer.StartSpan("test", opentracing.ChildOf(spanCtx))

	testCases := map[string]struct {
		ctx             context.Context
		expectedOk      bool
		expectedTraceID string
		expectedSpanID  string
	}{
		"no trace": {
			ctx:             context.Background(),
			expectedOk:      false,
			expectedTraceID: "",
			expectedSpanID:  "",
		},
		"trace": {
			ctx:             opentracing.ContextWithSpan(context.Background(), span),
			expectedOk:      true,
			expectedTraceID: expectedTraceID,
			expectedSpanID:  span.Context().(jaeger.SpanContext).SpanID().String(),
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			traceID, spanID, ok := ExtractTraceSpanID(tc.ctx)
			require.Equal(t, tc.expectedOk, ok)
			require.Equal(t, tc.expectedTraceID, traceID)
			require.Equal(t, tc.expectedSpanID, spanID)

			traceID, ok = ExtractTraceID(tc.ctx)
			require.Equal(t, tc.expectedOk, ok)
			require.Equal(t, tc.expectedTraceID, traceID)
		})
	}
}

func TestExtractTraceSpanID_OTel(t *testing.T) {
	const expectedSpanID = "0000000000000003"

	traceID, err := trace.TraceIDFromHex(expectedTraceID)
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex(expectedSpanID)
	require.NoError(t, err)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})

	testCases := map[string]struct {
		ctx             context.Context
		expectedOk      bool
		expectedTraceID string
		expectedSpanID  string
	}{
		"no trace": {
			ctx:             context.Background(),
			expectedOk:      false,
			expectedTraceID: "",
			expectedSpanID:  "",
		},
		"trace": {
			ctx:             trace.ContextWithSpanContext(context.Background(), sc),
			expectedOk:      true,
			expectedTraceID: expectedTraceID,
			expectedSpanID:  expectedSpanID,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			traceID, spanID, ok := ExtractTraceSpanID(tc.ctx)
			require.Equal(t, tc.expectedOk, ok)
			require.Equal(t, tc.expectedTraceID, traceID)
			require.Equal(t, tc.expectedSpanID, spanID)

			traceID, ok = ExtractTraceID(tc.ctx)
			require.Equal(t, tc.expectedOk, ok)
			require.Equal(t, tc.expectedTraceID, traceID)
		})
	}
}

func TestExtractSampledTraceID_OTel(t *testing.T) {
	traceID, err := trace.TraceIDFromHex(expectedTraceID)
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0000000000000003")
	require.NoError(t, err)

	t.Run("sampled", func(t *testing.T) {
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
		})
		ctx := trace.ContextWithSpanContext(context.Background(), sc)
		gotTraceID, sampled := ExtractSampledTraceID(ctx)
		require.True(t, sampled)
		require.Equal(t, expectedTraceID, gotTraceID)
	})

	t.Run("not sampled", func(t *testing.T) {
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  spanID,
		})
		ctx := trace.ContextWithSpanContext(context.Background(), sc)
		_, sampled := ExtractSampledTraceID(ctx)
		require.False(t, sampled)
	})

	t.Run("no span", func(t *testing.T) {
		_, sampled := ExtractSampledTraceID(context.Background())
		require.False(t, sampled)
	})
}
