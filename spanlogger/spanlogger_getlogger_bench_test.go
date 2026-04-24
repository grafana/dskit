package spanlogger

import (
	"context"
	"testing"

	"github.com/go-kit/log"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
)

// BenchmarkGetLogger benchmarks the getLogger path on SpanLogger.
func BenchmarkGetLogger(b *testing.B) {
	resolver := tenant.NewMultiResolver()

	// Most important: single log per short-lived SpanLogger (hits lazy init every time)
	b.Run("new_then_log", func(b *testing.B) {
		logger := noDebugNoopLogger{}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := New(context.Background(), logger, "test", resolver)
			_ = sl.Log("msg", "foo")
			sl.Finish()
		}
	})

	// DebugLog disabled (common production case) - no log output but still calls getLogger for span
	b.Run("new_then_debuglog_disabled", func(b *testing.B) {
		logger := noDebugNoopLogger{}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := New(context.Background(), logger, "test", resolver)
			sl.DebugLog("msg", "foo")
			sl.Finish()
		}
	})

	// FromContext path
	b.Run("from_ctx_then_log", func(b *testing.B) {
		logger := noDebugNoopLogger{}
		_, ctx := New(context.Background(), logger, "parent", resolver)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sl := FromContext(ctx, logger, resolver)
			_ = sl.Log("msg", "foo")
			sl.Finish()
		}
	})

	// Steady state (cached logger, many calls)
	b.Run("steady_state_log", func(b *testing.B) {
		logger := noDebugNoopLogger{}
		sl, _ := New(context.Background(), logger, "test", resolver)
		_ = sl.Log("warmup", "warmup")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = sl.Log("msg", "foo", "more", "data")
		}
	})

	// With tenant (triggers user field in logger)
	b.Run("with_tenant_log", func(b *testing.B) {
		logger := noDebugNoopLogger{}
		ctx := user.InjectOrgID(context.Background(), "user-123")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sl, _ := New(ctx, logger, "test", resolver)
			_ = sl.Log("msg", "foo")
			sl.Finish()
		}
	})

	// SetSpanAndLogTag then log
	b.Run("set_tag_then_log", func(b *testing.B) {
		logger := noDebugNoopLogger{}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := New(context.Background(), logger, "test", resolver)
			sl.SetSpanAndLogTag("extra_key", "extra_val")
			_ = sl.Log("msg", "foo")
			sl.Finish()
		}
	})

	// DebugLog enabled
	b.Run("debuglog_enabled", func(b *testing.B) {
		logger := loggerWithDebugEnabled{
			Logger:       log.NewNopLogger(),
			debugEnabled: true,
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := New(context.Background(), logger, "test", resolver)
			sl.DebugLog("msg", "foo", "more", "data")
			sl.Finish()
		}
	})
}
