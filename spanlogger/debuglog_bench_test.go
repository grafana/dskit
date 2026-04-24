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

// BenchmarkDebugLog benchmarks the DebugLog method under various conditions
// reflecting production usage patterns:
// - debug=off/sampled=off: the most common production case (ingester query path)
// - debug=on/sampled=off: debug logging enabled but trace is not sampled
//
// Each case is tested with different numbers of key-value pairs (2, 4, 8 args)
// matching real call sites in the ingester query path.
// We use literal arguments (not pre-built slices) to match how callers actually call DebugLog.
func BenchmarkDebugLog(b *testing.B) {
	resolver := tenant.NewMultiResolver()

	type testCase struct {
		name         string
		debugEnabled bool
		sampled      bool
	}

	cases := []testCase{
		{"debug=off_sampled=off", false, false},
		{"debug=on_sampled=off", true, false},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			sl := &SpanLogger{
				ctx:        context.Background(),
				resolver:   resolver,
				baseLogger: noDebugNoopLogger{},

				sampled:      tc.sampled,
				debugEnabled: tc.debugEnabled,
			}
			// Pre-initialize the logger to avoid one-time cost in the benchmark loop
			_ = sl.getLogger()

			// Use literal arguments to match how callers actually call DebugLog in production.
			// This ensures interface boxing happens at the call site, like it does in production code.
			b.Run("args=2", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					sl.DebugLog("msg", "foo")
				}
			})

			b.Run("args=4", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					sl.DebugLog("msg", "foo", "more", "data")
				}
			})

			b.Run("args=8", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					sl.DebugLog("msg", "foo", "more", "data", "series", 100, "samples", 200)
				}
			})
		})
	}
}

// BenchmarkDebugLogWithRealLogger uses a real go-kit logger (writing to a buffer)
// to measure the full cost of DebugLog including log formatting.
func BenchmarkDebugLogWithRealLogger(b *testing.B) {
	resolver := tenant.NewMultiResolver()

	type testCase struct {
		name         string
		debugEnabled bool
	}

	cases := []testCase{
		{"debug=off", false},
		{"debug=on", true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			buf := bytes.NewBuffer(make([]byte, 0, 4096))
			logger := dskit_log.NewGoKitWithWriter("logfmt", buf)
			logger = log.With(logger, "ts", log.DefaultTimestampUTC)

			if tc.debugEnabled {
				logger = level.NewFilter(logger, level.AllowAll())
			} else {
				logger = level.NewFilter(logger, level.AllowInfo())
			}

			logger = loggerWithDebugEnabled{
				Logger:       logger,
				debugEnabled: tc.debugEnabled,
			}

			sl, _ := New(context.Background(), logger, "test", resolver)
			// sampled is false by default (no tracing provider configured)

			b.Run("args=4", func(b *testing.B) {
				buf.Reset()
				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					sl.DebugLog("msg", "foo", "more", "data")
				}
			})

			b.Run("args=8", func(b *testing.B) {
				buf.Reset()
				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					sl.DebugLog("msg", "foo", "more", "data", "series", 100, "samples", 200)
				}
			})
		})
	}
}
