package spanlogger

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestOtelSpanLogger_Log(t *testing.T) {
	logger := log.NewNopLogger()
	resolver := fakeResolver{}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(tracetest.NewInMemoryExporter()),
	)
	span, ctx := OtelNew(context.Background(), tp.Tracer("test"), logger, "test", resolver, "bar")
	defer span.End()
	_ = span.Log("foo")
	newSpan := OtelFromContext(ctx, logger, resolver)
	require.Equal(t, span.Span, newSpan.Span)
	_ = newSpan.Log("bar")
	noSpan := OtelFromContext(context.Background(), logger, resolver)
	_ = noSpan.Log("foo")
	require.Error(t, noSpan.Error(errors.New("err")))
	require.NoError(t, noSpan.Error(nil))
}

func TestOtelSpanLogger_CustomLogger(t *testing.T) {
	var logged [][]interface{}
	var logger funcLogger = func(keyvals ...interface{}) error {
		logged = append(logged, keyvals)
		return nil
	}
	resolver := fakeResolver{}
	exp := tracetest.NewNoopExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		// Set the sampler to never sample so that traceID is not logged.
		sdktrace.WithSampler(sdktrace.NeverSample()),
	)
	span, ctx := OtelNew(context.Background(), tp.Tracer("test"), logger, "test", resolver)
	_ = span.Log("msg", "original spanlogger")

	span = OtelFromContext(ctx, log.NewNopLogger(), resolver)
	_ = span.Log("msg", "restored spanlogger")

	span = OtelFromContext(context.Background(), logger, resolver)
	_ = span.Log("msg", "fallback spanlogger")

	expect := [][]interface{}{
		{"method", "test", "msg", "original spanlogger"},
		{"msg", "restored spanlogger"},
		{"msg", "fallback spanlogger"},
	}

	require.Equal(t, expect, logged)
}

func TestOtelSpanCreatedWithTenantTag(t *testing.T) {
	exp, sp := createOtelSpan(user.InjectOrgID(context.Background(), "team-a"))
	defer sp.End()

	require.Equal(t, 1, len(exp.GetSpans().Snapshots()))
	require.Equal(t,
		[]attribute.KeyValue{attribute.StringSlice(TenantIDsTagName, []string{"team-a"})},
		exp.GetSpans().Snapshots()[0].Attributes())
}

func TestOtelSpanCreatedWithoutTenantTag(t *testing.T) {
	exp, sp := createOtelSpan(context.Background())
	defer sp.End()
	require.Equal(t, 1, len(exp.GetSpans().Snapshots()))

	exist := false
	for _, kv := range exp.GetSpans().Snapshots()[0].Attributes() {
		if kv.Key == TenantIDsTagName {
			exist = true
		}
	}
	require.False(t, exist)
}

// Using a no-op logger and no tracing provider, measure the overhead of a small log call.
func BenchmarkOtelSpanLogger(b *testing.B) {
	_, sl := createOtelSpan(context.Background())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sl.Log("msg", "foo", "more", "data")
	}
}

func createOtelSpan(ctx context.Context) (*tracetest.InMemoryExporter, *OtelSpanLogger) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
	)
	otel.SetTracerProvider(tp)
	tt := tp.Tracer("test")

	sl, _ := OtelNew(ctx, tt, log.NewNopLogger(), "get", fakeResolver{})
	// Force flush to ensure spans are reported before the test ends.
	tp.ForceFlush(ctx)
	return exp, sl
}
