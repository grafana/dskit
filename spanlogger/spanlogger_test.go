package spanlogger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	dskit_log "github.com/grafana/dskit/log"
	"github.com/grafana/dskit/user"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
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

	_, thisFile, thisLineNumber, ok := runtime.Caller(0)
	require.True(t, ok)

	span, ctx := New(context.Background(), logger, "test", resolver)
	_ = span.Log("msg", "original spanlogger")

	span = FromContext(ctx, log.NewNopLogger(), resolver)
	_ = span.Log("msg", "restored spanlogger")

	span = FromContext(context.Background(), logger, resolver)
	_ = span.Log("msg", "fallback spanlogger")

	expect := [][]interface{}{
		{"method", "test", "caller", toCallerInfo(thisFile, thisLineNumber+4), "msg", "original spanlogger"},
		{"caller", toCallerInfo(thisFile, thisLineNumber+7), "msg", "restored spanlogger"},
		{"caller", toCallerInfo(thisFile, thisLineNumber+10), "msg", "fallback spanlogger"},
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

func TestSpanLogger_SetSpanAndLogTag(t *testing.T) {
	mockTracer := mocktracer.New()
	opentracing.SetGlobalTracer(mockTracer)

	logMessages := [][]interface{}{}
	var logger funcLogger = func(keyvals ...interface{}) error {
		logMessages = append(logMessages, keyvals)
		return nil
	}

	_, thisFile, thisLineNumber, ok := runtime.Caller(0)
	require.True(t, ok)

	spanLogger, _ := New(context.Background(), logger, "the_method", fakeResolver{})
	require.NoError(t, spanLogger.Log("msg", "this is the first message"))

	spanLogger.SetSpanAndLogTag("id", "123")
	require.NoError(t, spanLogger.Log("msg", "this is the second message"))

	spanLogger.SetSpanAndLogTag("more context", "abc")
	require.NoError(t, spanLogger.Log("msg", "this is the third message"))

	span := spanLogger.Span.(*mocktracer.MockSpan)
	expectedTags := map[string]interface{}{
		"id":           "123",
		"more context": "abc",
	}
	require.Equal(t, expectedTags, span.Tags())

	expectedLogMessages := [][]interface{}{
		{
			"method", "the_method",
			"caller", toCallerInfo(thisFile, thisLineNumber+4),
			"msg", "this is the first message",
		},
		{
			"method", "the_method",
			"caller", toCallerInfo(thisFile, thisLineNumber+7),
			"id", "123",
			"msg", "this is the second message",
		},
		{
			"method", "the_method",
			"caller", toCallerInfo(thisFile, thisLineNumber+10),
			"id", "123",
			"more context", "abc",
			"msg", "this is the third message",
		},
	}

	require.Equal(t, expectedLogMessages, logMessages)
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

func BenchmarkSpanLoggerWithRealLogger(b *testing.B) {
	testCases := map[string]bool{
		"all levels allowed":     true,
		"info and above allowed": false,
	}

	for name, debugEnabled := range testCases {
		b.Run(name, func(b *testing.B) {
			buf := bytes.NewBuffer(nil)
			logger := dskit_log.NewGoKitWithWriter("logfmt", buf)
			logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", Caller(5))

			if debugEnabled {
				logger = level.NewFilter(logger, level.AllowAll())
			} else {
				logger = level.NewFilter(logger, level.AllowInfo())
			}

			logger = loggerWithDebugEnabled{
				Logger:       logger,
				debugEnabled: debugEnabled,
			}

			resolver := fakeResolver{}
			sl, _ := New(context.Background(), logger, "test", resolver, "bar")

			b.Run("Log", func(b *testing.B) {
				buf.Reset()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					_ = sl.Log("msg", "foo", "more", "data")
				}
			})

			b.Run("level.Debug", func(b *testing.B) {
				buf.Reset()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					_ = level.Debug(sl).Log("msg", "foo", "more", "data")
				}
			})

			b.Run("DebugLog", func(b *testing.B) {
				buf.Reset()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					sl.DebugLog("msg", "foo", "more", "data")
				}
			})
		})
	}
}

func BenchmarkSpanLoggerAwareCaller(b *testing.B) {
	runBenchmark := func(b *testing.B, caller log.Valuer) {
		buf := bytes.NewBuffer(nil)
		logger := dskit_log.NewGoKitWithWriter("logfmt", buf)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", caller)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = logger.Log("msg", "foo", "more", "data")
		}

	}

	defaultStackDepth := 5

	b.Run("with go-kit's Caller", func(b *testing.B) {
		runBenchmark(b, log.Caller(defaultStackDepth))
	})

	b.Run("with dskit's spanlogger.Caller", func(b *testing.B) {
		runBenchmark(b, Caller(defaultStackDepth))
	})
}

// Logger which does nothing and implements the DebugEnabled interface used by SpanLogger.
type noDebugNoopLogger struct{}

func (noDebugNoopLogger) Log(...interface{}) error { return nil }
func (noDebugNoopLogger) DebugEnabled() bool       { return false }

// Logger which delegates to the inner log.Logger, and implements the DebugEnabled interface used by SpanLogger.
type loggerWithDebugEnabled struct {
	log.Logger
	debugEnabled bool
}

func (l loggerWithDebugEnabled) DebugEnabled() bool { return l.debugEnabled }

func TestSpanLoggerAwareCaller(t *testing.T) {
	testCases := map[string]func(w io.Writer) log.Logger{
		// This is based on Mimir's default logging configuration: https://github.com/grafana/mimir/blob/50d1c27b4ad82b265ff5a865345bec2d726f64ef/pkg/util/log/log.go#L45-L46
		"default logger": func(w io.Writer) log.Logger {
			logger := dskit_log.NewGoKitWithWriter("logfmt", w)
			logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", Caller(5))
			logger = level.NewFilter(logger, level.AllowAll())
			return logger
		},

		// This is based on Mimir's logging configuration with rate-limiting enabled: https://github.com/grafana/mimir/blob/50d1c27b4ad82b265ff5a865345bec2d726f64ef/pkg/util/log/log.go#L42-L43
		"rate-limited logger": func(w io.Writer) log.Logger {
			logger := dskit_log.NewGoKitWithWriter("logfmt", w)
			logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", Caller(6))
			logger = dskit_log.NewRateLimitedLogger(logger, 1000, 1000, nil)
			logger = level.NewFilter(logger, level.AllowAll())
			return logger
		},

		"default logger that has been wrapped with further information": func(w io.Writer) log.Logger {
			logger := dskit_log.NewGoKitWithWriter("logfmt", w)
			logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", Caller(5))
			logger = level.NewFilter(logger, level.AllowAll())
			logger = log.With(logger, "user", "user-1")
			return logger
		},
	}

	resolver := fakeResolver{}

	setupTest := func(t *testing.T, loggerFactory func(io.Writer) log.Logger) (*bytes.Buffer, *SpanLogger, *jaeger.Span) {
		reporter := jaeger.NewInMemoryReporter()
		tracer, closer := jaeger.NewTracer(
			"test",
			jaeger.NewConstSampler(true),
			reporter,
		)
		t.Cleanup(func() { _ = closer.Close() })

		span, ctx := opentracing.StartSpanFromContextWithTracer(context.Background(), tracer, "test")

		buf := bytes.NewBuffer(nil)
		logger := loggerFactory(buf)
		spanLogger := FromContext(ctx, logger, resolver)

		return buf, spanLogger, span.(*jaeger.Span)
	}

	requireSpanHasTwoLogLinesWithoutCaller := func(t *testing.T, span *jaeger.Span, extraFields ...otlog.Field) {
		logs := span.Logs()
		require.Len(t, logs, 2)

		expectedFields := append(slices.Clone(extraFields), otlog.String("msg", "this is a test"))
		require.Equal(t, expectedFields, logs[0].Fields)

		expectedFields = append(slices.Clone(extraFields), otlog.String("msg", "this is another test"))
		require.Equal(t, expectedFields, logs[1].Fields)
	}

	for name, loggerFactory := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Run("logging with Log()", func(t *testing.T) {
				logs, spanLogger, span := setupTest(t, loggerFactory)

				_, thisFile, lineNumberTwoLinesBeforeFirstLogCall, ok := runtime.Caller(0)
				require.True(t, ok)
				_ = spanLogger.Log("msg", "this is a test")

				logged := logs.String()
				require.Contains(t, logged, "caller="+toCallerInfo(thisFile, lineNumberTwoLinesBeforeFirstLogCall+2))
				require.Equalf(t, 1, strings.Count(logged, "caller="), "expected to only have one caller field, but got: %v", logged)

				logs.Reset()
				_, _, lineNumberTwoLinesBeforeSecondLogCall, ok := runtime.Caller(0)
				require.True(t, ok)
				_ = spanLogger.Log("msg", "this is another test")

				logged = logs.String()
				require.Contains(t, logged, "caller="+toCallerInfo(thisFile, lineNumberTwoLinesBeforeSecondLogCall+2))
				require.Equalf(t, 1, strings.Count(logged, "caller="), "expected to only have one caller field, but got: %v", logged)

				requireSpanHasTwoLogLinesWithoutCaller(t, span)
			})

			t.Run("logging with DebugLog()", func(t *testing.T) {
				logs, spanLogger, span := setupTest(t, loggerFactory)
				_, thisFile, lineNumberTwoLinesBeforeLogCall, ok := runtime.Caller(0)
				require.True(t, ok)
				spanLogger.DebugLog("msg", "this is a test")

				logged := logs.String()
				require.Contains(t, logged, "caller="+toCallerInfo(thisFile, lineNumberTwoLinesBeforeLogCall+2))
				require.Equalf(t, 1, strings.Count(logged, "caller="), "expected to only have one caller field, but got: %v", logged)

				logs.Reset()
				_, _, lineNumberTwoLinesBeforeSecondLogCall, ok := runtime.Caller(0)
				require.True(t, ok)
				spanLogger.DebugLog("msg", "this is another test")

				logged = logs.String()
				require.Contains(t, logged, "caller="+toCallerInfo(thisFile, lineNumberTwoLinesBeforeSecondLogCall+2))
				require.Equalf(t, 1, strings.Count(logged, "caller="), "expected to only have one caller field, but got: %v", logged)

				requireSpanHasTwoLogLinesWithoutCaller(t, span)
			})

			t.Run("logging with SpanLogger wrapped in a level", func(t *testing.T) {
				logs, spanLogger, span := setupTest(t, loggerFactory)

				_, thisFile, lineNumberTwoLinesBeforeFirstLogCall, ok := runtime.Caller(0)
				require.True(t, ok)
				_ = level.Info(spanLogger).Log("msg", "this is a test")

				logged := logs.String()
				require.Contains(t, logged, "caller="+toCallerInfo(thisFile, lineNumberTwoLinesBeforeFirstLogCall+2))
				require.Equalf(t, 1, strings.Count(logged, "caller="), "expected to only have one caller field, but got: %v", logged)

				logs.Reset()
				_, _, lineNumberTwoLinesBeforeSecondLogCall, ok := runtime.Caller(0)
				require.True(t, ok)
				_ = level.Info(spanLogger).Log("msg", "this is another test")

				logged = logs.String()
				require.Contains(t, logged, "caller="+toCallerInfo(thisFile, lineNumberTwoLinesBeforeSecondLogCall+2))
				require.Equalf(t, 1, strings.Count(logged, "caller="), "expected to only have one caller field, but got: %v", logged)

				requireSpanHasTwoLogLinesWithoutCaller(t, span, otlog.String("level", "info"))
			})
		})
	}
}

func toCallerInfo(path string, lineNumber int) string {
	fileName := filepath.Base(path)

	return fmt.Sprintf("%s:%v", fileName, lineNumber)
}
