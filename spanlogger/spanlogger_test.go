package spanlogger

import (
	"context"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/user"
)

func TestSpanLogger_Log(t *testing.T) {
	logger := log.NewNopLogger()
	resolver := fakeResolver{}
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

func TestSpanLogger_CustomLogger(t *testing.T) {
	var logged [][]interface{}
	var logger funcLogger = func(keyvals ...interface{}) error {
		logged = append(logged, keyvals)
		return nil
	}
	resolver := fakeResolver{}
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

func TestSpanCreatedWithTenantTag(t *testing.T) {
	mockSpan := createSpan(user.InjectOrgID(context.Background(), "team-a"))

	require.Equal(t, []string{"team-a"}, mockSpan.Tag(TenantIDsTagName))
}

func TestSpanCreatedWithoutTenantTag(t *testing.T) {
	mockSpan := createSpan(context.Background())

	_, exists := mockSpan.Tags()[TenantIDsTagName]
	require.False(t, exists)
}

func createSpan(ctx context.Context) *mocktracer.MockSpan {
	mockTracer := mocktracer.New()
	opentracing.SetGlobalTracer(mockTracer)

	logger, _ := New(ctx, log.NewNopLogger(), "name", fakeResolver{})
	return logger.Span.(*mocktracer.MockSpan)
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
