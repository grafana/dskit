// SPDX-License-Identifier: Apache-2.0

package tracing

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	jaeger "github.com/uber/jaeger-client-go"
)

func TestExtractTraceSpanID(t *testing.T) {
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
			expectedTraceID: "00000000000000010000000000000002",
			expectedSpanID:  span.Context().(jaeger.SpanContext).SpanID().String(),
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			traceID, spanID, ok := ExtractTraceSpanID(tc.ctx)
			require.Equal(t, tc.expectedOk, ok)
			require.Equal(t, tc.expectedTraceID, traceID)
			require.Equal(t, tc.expectedSpanID, spanID)
		})
	}
}
