package spanlogger

import (
	"bytes"
	"context"
	"testing"

	dskit_log "github.com/grafana/dskit/log"
	"github.com/grafana/dskit/tenant"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type funcLogger func(keyvals ...interface{}) error

func (f funcLogger) Log(keyvals ...interface{}) error { return f(keyvals...) }

// Using a no-op logger and no tracing provider, measure the overhead of a small log call.
func BenchmarkSpanLogger(b *testing.B) {
	logger := noDebugNoopLogger{}
	resolver := tenant.NewMultiResolver()
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

			resolver := tenant.NewMultiResolver()
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

	const defaultStackDepth = 5

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
