package spanprofiler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
)

func TestSampledTraceID(t *testing.T) {
	for _, tc := range []struct {
		name    string
		traceID jaeger.TraceID
		sampled bool
		want    string
	}{
		{
			name:    "64-bit trace ID is zero-padded to 32 characters",
			traceID: jaeger.TraceID{High: 0, Low: 0x1},
			sampled: true,
			want:    "00000000000000000000000000000001",
		},
		{
			name:    "128-bit trace ID",
			traceID: jaeger.TraceID{High: 0xabc, Low: 0xdef},
			sampled: true,
			want:    "0000000000000abc0000000000000def",
		},
		{
			name:    "unsampled trace returns empty string",
			traceID: jaeger.TraceID{High: 0xabc, Low: 0xdef},
			sampled: false,
			want:    "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spanCtx := jaeger.NewSpanContext(tc.traceID, jaeger.SpanID(1), jaeger.SpanID(0), tc.sampled, nil)
			require.Equal(t, tc.want, sampledTraceID(spanCtx))
		})
	}
}

func TestSpanProfiler_pprof_labels_propagation(t *testing.T) {
	tt := initTestTracer(t, 1)
	defer func() { require.NoError(t, tt.Close()) }()

	t.Run("pprof labels are not propagated to child spans", func(t *testing.T) {
		rootSpan, ctx := StartSpanFromContext(context.Background(), "RootSpan")
		defer rootSpan.Finish()
		rootLabels := spanPprofLabels(rootSpan)
		require.Equal(t, rootLabels["span_name"], "RootSpan")
		rootSpanID, err := jaeger.SpanIDFromString(rootLabels["span_id"])
		require.NoError(t, err)

		// Regardless of anything, pprof labels are attached to the current
		// goroutine, and the "pyroscope.profile.id" tag is set.
		childSpan, _ := StartSpanFromContext(ctx, "ChildSpan")
		defer childSpan.Finish()
		childLabels := spanPprofLabels(childSpan)
		require.Equal(t, childLabels["span_name"], "ChildSpan")
		childSpanID, err := jaeger.SpanIDFromString(childLabels["span_id"])
		require.NoError(t, err)

		require.NotEqual(t, rootSpanID, childSpanID)
		require.Equal(t, childSpanID.String(), spanTags(childSpan)["pyroscope.profile.id"])
	})
}
