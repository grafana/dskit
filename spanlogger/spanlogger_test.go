package spanlogger

import (
	"context"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
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
	otel.SetTracerProvider(tp)
	span, ctx := New(context.Background(), logger, "test", resolver, "bar")
	_ = span.Log("foo")
	newSpan := FromContext(ctx, logger, resolver)
	require.Equal(t, span.Span, newSpan.Span)
	_ = newSpan.Log("bar")
	noSpan := FromContext(context.Background(), logger, resolver)
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
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		// Set the sampler to never sample so that traceID is not logged.
		sdktrace.WithSampler(sdktrace.NeverSample()),
	)
	otel.SetTracerProvider(tp)
	span, ctx := New(context.Background(), logger, "test", resolver)
	_ = span.Log("msg", "original spanlogger")

	span = FromContext(ctx, log.NewNopLogger(), resolver)
	_ = span.Log("msg", "restored spanlogger")

	span = FromContext(context.Background(), logger, resolver)
	_ = span.Log("msg", "fallback spanlogger")

	expect := [][]interface{}{
		{"method", "test", "msg", "original spanlogger"},
		{"msg", "restored spanlogger"},
		{"msg", "fallback spanlogger"},
	}
	require.Equal(t, expect, logged)
}

func TestOtelSpanCreatedWithTenantTag(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "team-a")
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	sl, ctx := New(ctx, log.NewNopLogger(), "get", fakeResolver{})
	sl.End()
	// // Force flush to ensure spans are reported before the test ends.
	tp.ForceFlush(ctx)

	require.Equal(t, 1, len(exp.GetSpans().Snapshots()))
	require.Equal(t,
		[]attribute.KeyValue{attribute.StringSlice(TenantIDsTagName, []string{"team-a"})},
		exp.GetSpans().Snapshots()[0].Attributes())
}

func TestOtelSpanCreatedWithoutTenantTag(t *testing.T) {
	exp, _ := createOtelSpan(context.Background())
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

func createOtelSpan(ctx context.Context) (*tracetest.InMemoryExporter, *SpanLogger) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	sl, _ := New(ctx, log.NewNopLogger(), "get", fakeResolver{})
	sl.Span.End()
	// Force flush to ensure spans are reported before the test ends.
	tp.ForceFlush(ctx)
	return exp, sl
}

type funcLogger func(keyvals ...interface{}) error

func (f funcLogger) Log(keyvals ...interface{}) error {
	return f(keyvals...)
}

type fakeResolver struct {
}

func (fakeResolver) TenantID(ctx context.Context) (string, error) {
	id, err := user.ExtractOrgID(ctx)
	if err != nil {
		return "", err
	}

	// handle the relative reference to current and parent path.
	if id == "." || id == ".." || strings.ContainsAny(id, `\/`) {
		return "", nil
	}

	return id, nil
}

func (r fakeResolver) TenantIDs(ctx context.Context) ([]string, error) {
	id, err := r.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, nil
	}

	return []string{id}, nil
}

// Using a no-op logger and no tracing provider, measure the overhead of a small log call.
func BenchmarkSpanLogger(b *testing.B) {
	logger := noDebugNoopLogger{}
	resolver := fakeResolver{}
	sl, _ := New(context.Background(), logger, "test", resolver, "bar")
	b.Run("log", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = sl.Log("msg", "foo", "more", "data")
		}
	})
	b.Run("level.debug", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = level.Debug(sl).Log("msg", "foo", "more", "data")
		}
	})
	b.Run("debuglog", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sl.DebugLog("msg", "foo", "more", "data")
		}
	})
}

// Logger which does nothing and implements the DebugEnabled interface used by SpanLogger.
type noDebugNoopLogger struct{}

func (noDebugNoopLogger) Log(...interface{}) error { return nil }
func (noDebugNoopLogger) DebugEnabled() bool       { return false }
