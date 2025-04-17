package spanlogger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"

	dskit_log "github.com/grafana/dskit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
)

var mockTracer = mocktracer.New()

func init() { opentracing.SetGlobalTracer(mockTracer) }

func TestOpenTracingSpanLogger_Log(t *testing.T) {
	logger := log.NewNopLogger()
	resolver := tenant.NewMultiResolver()
	span, ctx := New(context.Background(), logger, "test", resolver, "bar")
	_ = span.Log("foo")
	newSpan := FromContext(ctx, logger, resolver)
	require.Equal(t, span.opentracingSpan, newSpan.opentracingSpan)
	_ = newSpan.Log("bar")
	noSpan := FromContext(context.Background(), logger, resolver)
	_ = noSpan.Log("foo")
	require.Error(t, noSpan.Error(errors.New("err")))
	require.NoError(t, noSpan.Error(nil))
}

func TestOpenTracingSpanLogger_CustomLogger(t *testing.T) {
	var logged [][]interface{}
	var logger funcLogger = func(keyvals ...interface{}) error {
		logged = append(logged, keyvals)
		return nil
	}
	resolver := tenant.NewMultiResolver()

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

func TestOpenTracingSpanLogger_SetSpanAndLogTag(t *testing.T) {
	logMessages := [][]interface{}{}
	var logger funcLogger = func(keyvals ...interface{}) error {
		logMessages = append(logMessages, keyvals)
		return nil
	}

	spanLogger, _ := New(context.Background(), logger, "the_method", tenant.NewMultiResolver())
	require.NoError(t, spanLogger.Log("msg", "this is the first message"))

	spanLogger.SetSpanAndLogTag("id", "123")
	require.NoError(t, spanLogger.Log("msg", "this is the second message"))

	spanLogger.SetSpanAndLogTag("more context", "abc")
	require.NoError(t, spanLogger.Log("msg", "this is the third message"))

	span := spanLogger.opentracingSpan.(*mocktracer.MockSpan)
	expectedTags := map[string]interface{}{
		"id":           "123",
		"more context": "abc",
	}
	require.Equal(t, expectedTags, span.Tags())

	expectedLogMessages := [][]interface{}{
		{
			"method", "the_method",
			"msg", "this is the first message",
		},
		{
			"method", "the_method",
			"id", "123",
			"msg", "this is the second message",
		},
		{
			"method", "the_method",
			"id", "123",
			"more context", "abc",
			"msg", "this is the third message",
		},
	}

	require.Equal(t, expectedLogMessages, logMessages)
}

func TestOpenTracingSpanCreatedWithTenantTag(t *testing.T) {
	mockSpan := createOpenTracingMockSpan(user.InjectOrgID(context.Background(), "team-a"))

	require.Equal(t, []string{"team-a"}, mockSpan.Tag(TenantIDsTagName))
}

func TestOpenTracingSpanCreatedWithoutTenantTag(t *testing.T) {
	mockSpan := createOpenTracingMockSpan(context.Background())

	_, exists := mockSpan.Tags()[TenantIDsTagName]
	require.False(t, exists)
}

func createOpenTracingMockSpan(ctx context.Context) *mocktracer.MockSpan {
	logger, _ := New(ctx, log.NewNopLogger(), "name", tenant.NewMultiResolver())
	return logger.opentracingSpan.(*mocktracer.MockSpan)
}

func TestOpenTracingSpanLoggerAwareCaller(t *testing.T) {
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

	resolver := tenant.NewMultiResolver()

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

	toCallerInfo := func(path string, lineNumber int) string {
		fileName := filepath.Base(path)
		return fmt.Sprintf("%s:%v", fileName, lineNumber)
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
