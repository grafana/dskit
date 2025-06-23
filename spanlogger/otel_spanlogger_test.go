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
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	dskit_log "github.com/grafana/dskit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
)

var (
	tracer       = otel.Tracer("dskit/spanlogger.test")
	spanExporter = tracetest.NewInMemoryExporter()
)

func init() {
	otel.SetTracerProvider(tracesdk.NewTracerProvider(tracesdk.WithSpanProcessor(tracesdk.NewSimpleSpanProcessor(spanExporter))))
}

func TestOTelSpanLogger_New_FromContext_Finish(t *testing.T) {
	t.Cleanup(spanExporter.Reset)

	logger := log.NewNopLogger()
	resolver := tenant.NewMultiResolver()

	span, ctx := NewOTel(context.Background(), logger, tracer, "test", resolver, "bar")
	require.Nil(t, span.opentracingSpan)
	require.NotNil(t, span.otelSpan)
	require.Len(t, spanExporter.GetSpans(), 0, "There should be no spans exported before the span is finished")

	spanFromContext := FromContext(ctx, logger, resolver)
	require.Nil(t, spanFromContext.opentracingSpan)
	require.NotNil(t, spanFromContext.otelSpan)

	require.Equal(t, span.otelSpan.SpanContext().TraceID(), spanFromContext.otelSpan.SpanContext().TraceID(), "Span from context should have the same Trace ID")
	require.Len(t, spanExporter.GetSpans(), 0, "There should be no spans exported before the span is finished")

	span.Finish()
	require.Len(t, spanExporter.GetSpans(), 1, "There should be exactly one span after the span is finished")
}

func TestOTelSpanLogger_Log(t *testing.T) {
	t.Cleanup(spanExporter.Reset)

	logger := log.NewNopLogger()
	resolver := tenant.NewMultiResolver()
	span, ctx := NewOTel(context.Background(), logger, tracer, "test", resolver, "bar")
	_ = span.Log("foo")

	newSpan := FromContext(ctx, logger, resolver)

	require.Equal(t, span.opentracingSpan, newSpan.opentracingSpan)
	_ = newSpan.Log("bar")
	spanFromContext := FromContext(context.Background(), logger, resolver)
	_ = spanFromContext.Log("foo")
	require.Error(t, spanFromContext.Error(errors.New("err")))
	require.NoError(t, spanFromContext.Error(nil))
}

func TestOTelSpanLogger_Error(t *testing.T) {
	t.Cleanup(spanExporter.Reset)

	logger := log.NewNopLogger()
	resolver := tenant.NewMultiResolver()
	span, _ := NewOTel(context.Background(), logger, tracer, "test", resolver, "bar")

	require.Error(t, span.Error(errors.New("err")))
	require.NoError(t, span.Error(nil))
	span.Finish()

	spans := spanExporter.GetSpans()
	require.Len(t, spans, 1, "There should be exactly one span after the span is finished")
	exportedSpan := spans[0]
	require.Len(t, exportedSpan.Events, 2, "There should be one log and one exception event")
	require.Equal(t, codes.Error, exportedSpan.Status.Code)
	require.Equal(t, "log", exportedSpan.Events[0].Name)
	require.Equal(t, "exception", exportedSpan.Events[1].Name)
}

func TestOTelSpanLogger_SetError(t *testing.T) {
	t.Cleanup(spanExporter.Reset)

	logger := log.NewNopLogger()
	resolver := tenant.NewMultiResolver()
	span, _ := NewOTel(context.Background(), logger, tracer, "test", resolver, "bar")

	span.SetError()
	span.Finish()

	spans := spanExporter.GetSpans()
	require.Len(t, spans, 1, "There should be exactly one span after the span is finished")
	exportedSpan := spans[0]
	require.Equal(t, codes.Error, exportedSpan.Status.Code)
}

func TestOTelSpanLogger_CustomLogger(t *testing.T) {
	t.Cleanup(spanExporter.Reset)

	var expectedTraceID string
	var logged []map[string]string
	// logger will store all non-"trace_id" keys in `logged`
	// it will check that "trace_id" is equal to expectedTraceID.
	var logger funcLogger = func(keyvals ...interface{}) error {
		values := map[string]string{}
		var loggedTraceID string
		for i := 0; i < len(keyvals); i += 2 {
			k := keyvals[i].(string)
			v := keyvals[i+1].(string) // we only log strings.
			if k == "trace_id" {
				loggedTraceID = v
			} else {
				values[k] = v
			}
		}
		require.Equal(t, expectedTraceID, loggedTraceID)
		logged = append(logged, values)
		return nil
	}
	resolver := tenant.NewMultiResolver()

	span, ctx := NewOTel(context.Background(), logger, tracer, "test", resolver)
	expectedTraceID = trace.SpanFromContext(ctx).SpanContext().TraceID().String()
	_ = span.Log("msg", "original spanlogger")

	span = FromContext(ctx, log.NewNopLogger(), resolver)
	_ = span.Log("msg", "restored spanlogger")

	// No trace_id expected for the next one.
	expectedTraceID = ""
	span = FromContext(context.Background(), logger, resolver)
	_ = span.Log("msg", "fallback spanlogger")

	expect := []map[string]string{
		{"method": "test", "msg": "original spanlogger"},
		{"msg": "restored spanlogger"},
		{"msg": "fallback spanlogger"},
	}
	require.Equal(t, expect, logged)
}

func TestOTelSpanLogger_SetSpanAndLogTag(t *testing.T) {
	t.Cleanup(spanExporter.Reset)

	var expectedTraceID string
	var logged []map[string]string
	// logger will store all non-"trace_id" keys in `logged`
	// it will check that "trace_id" is equal to expectedTraceID.
	var logger funcLogger = func(keyvals ...interface{}) error {
		values := map[string]string{}
		var loggedTraceID string
		for i := 0; i < len(keyvals); i += 2 {
			k := keyvals[i].(string)
			v := keyvals[i+1].(string) // we only log strings.
			if k == "trace_id" {
				loggedTraceID = v
			} else {
				values[k] = v
			}
		}
		require.Equal(t, expectedTraceID, loggedTraceID)
		logged = append(logged, values)
		return nil
	}

	spanLogger, ctx := NewOTel(context.Background(), logger, tracer, "the_method", tenant.NewMultiResolver())
	expectedTraceID = trace.SpanFromContext(ctx).SpanContext().TraceID().String()
	require.NoError(t, spanLogger.Log("msg", "this is the first message"))

	spanLogger.SetSpanAndLogTag("id", "123")
	require.NoError(t, spanLogger.Log("msg", "this is the second message"))

	spanLogger.SetSpanAndLogTag("more context", "abc")
	require.NoError(t, spanLogger.Log("msg", "this is the third message"))

	spanLogger.Finish()
	spans := spanExporter.GetSpans()
	require.Len(t, spans, 1, "There should be exactly one span after the span is finished")
	exportedSpan := spans[0]

	expectedAttributes := []attribute.KeyValue{
		attribute.String("id", "123"),
		attribute.String("more context", "abc"),
	}
	require.Equal(t, expectedAttributes, exportedSpan.Attributes)

	expectedLogMessages := []map[string]string{
		{
			"method": "the_method",
			"msg":    "this is the first message",
		},
		{
			"method": "the_method",
			"id":     "123",
			"msg":    "this is the second message",
		},
		{
			"method":       "the_method",
			"id":           "123",
			"more context": "abc",
			"msg":          "this is the third message",
		},
	}

	require.Equal(t, expectedLogMessages, logged)
}

func TestOTelSpanCreatedWithTenantAttribute(t *testing.T) {
	t.Cleanup(spanExporter.Reset)

	ctx := user.InjectOrgID(context.Background(), "team-a")
	sp, _ := NewOTel(ctx, log.NewNopLogger(), tracer, "name", tenant.NewMultiResolver())
	sp.Finish()

	spans := spanExporter.GetSpans()
	require.Len(t, spans, 1, "There should be exactly one span after the span is finished")
	exportedSpan := spans[0]

	require.Equal(t, []attribute.KeyValue{attribute.StringSlice(TenantIDsTagName, []string{"team-a"})}, exportedSpan.Attributes)
}

func TestOTelSpanCreatedWithMultipleTenantsAttribute(t *testing.T) {
	t.Cleanup(spanExporter.Reset)

	ctx := user.InjectOrgID(context.Background(), "team-a|team-b")
	sp, _ := NewOTel(ctx, log.NewNopLogger(), tracer, "name", tenant.NewMultiResolver())
	sp.Finish()

	spans := spanExporter.GetSpans()
	require.Len(t, spans, 1, "There should be exactly one span after the span is finished")
	exportedSpan := spans[0]

	require.Equal(t, []attribute.KeyValue{attribute.StringSlice(TenantIDsTagName, []string{"team-a", "team-b"})}, exportedSpan.Attributes)
}

func TestOTelSpanCreatedWithoutTenantAttribute(t *testing.T) {
	t.Cleanup(spanExporter.Reset)

	sp, _ := NewOTel(context.Background(), log.NewNopLogger(), tracer, "name", tenant.NewMultiResolver())
	sp.Finish()

	spans := spanExporter.GetSpans()
	require.Len(t, spans, 1, "There should be exactly one span after the span is finished")
	exportedSpan := spans[0]

	require.Empty(t, exportedSpan.Attributes)
}

func TestOTelSpanLoggerAwareCaller(t *testing.T) {
	testCases := map[string]func(w io.Writer) log.Logger{
		// This is based on Mimir's default logging configuration: https://github.com/grafana/mimir/blob/50d1c27b4ad82b265ff5a865345bec2d726f64ef/pkg/util/log/log.go#L45-L46
		"default logger": func(w io.Writer) log.Logger {
			logger := dskit_log.NewGoKitWithWriter("logfmt", w)
			logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", Caller(5))
			logger = level.NewFilter(logger, level.AllowAll())
			return logger
		},

		//This is based on Mimir's logging configuration with rate-limiting enabled: https://github.com/grafana/mimir/blob/50d1c27b4ad82b265ff5a865345bec2d726f64ef/pkg/util/log/log.go#L42-L43
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

	setupTest := func(t *testing.T, loggerFactory func(io.Writer) log.Logger) (*bytes.Buffer, *SpanLogger) {
		t.Cleanup(spanExporter.Reset)

		ctx, _ := tracer.Start(context.Background(), "test")

		buf := bytes.NewBuffer(nil)
		logger := loggerFactory(buf)
		spanLogger := FromContext(ctx, logger, resolver)
		require.NotNil(t, spanLogger.otelSpan)
		require.Nil(t, spanLogger.opentracingSpan)

		return buf, spanLogger
	}

	requireSpanHasTwoLogLinesWithoutCaller := func(t *testing.T, span tracetest.SpanStub, extraAttributes ...attribute.KeyValue) {
		evs := span.Events
		require.Len(t, evs, 2)

		require.Equal(t, "log", evs[0].Name)
		require.Equal(t, "log", evs[1].Name)
		require.Equal(t, append(slices.Clone(extraAttributes), attribute.String("msg", "this is a test")), evs[0].Attributes)
		require.Equal(t, append(slices.Clone(extraAttributes), attribute.String("msg", "this is another test")), evs[1].Attributes)
	}

	toCallerInfo := func(path string, lineNumber int) string {
		fileName := filepath.Base(path)
		return fmt.Sprintf("%s:%v", fileName, lineNumber)
	}

	for name, loggerFactory := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Run("logging with Log()", func(t *testing.T) {
				t.Cleanup(spanExporter.Reset)

				logs, spanLogger := setupTest(t, loggerFactory)

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

				spanLogger.Finish()
				exportedSpans := spanExporter.GetSpans()
				require.Len(t, exportedSpans, 1, "There should be exactly one span after the span is finished")
				requireSpanHasTwoLogLinesWithoutCaller(t, exportedSpans[0])
			})

			t.Run("logging with DebugLog()", func(t *testing.T) {
				t.Cleanup(spanExporter.Reset)

				logs, spanLogger := setupTest(t, loggerFactory)

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

				spanLogger.Finish()
				exportedSpans := spanExporter.GetSpans()
				require.Len(t, exportedSpans, 1, "There should be exactly one span after the span is finished")
				requireSpanHasTwoLogLinesWithoutCaller(t, exportedSpans[0])
			})

			t.Run("logging with SpanLogger wrapped in a level", func(t *testing.T) {
				t.Cleanup(spanExporter.Reset)

				logs, spanLogger := setupTest(t, loggerFactory)

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

				spanLogger.Finish()
				exportedSpans := spanExporter.GetSpans()
				require.Len(t, exportedSpans, 1, "There should be exactly one span after the span is finished")
				requireSpanHasTwoLogLinesWithoutCaller(t, exportedSpans[0], attribute.String("level", "info"))
			})
		})
	}
}
