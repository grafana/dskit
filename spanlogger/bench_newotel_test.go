package spanlogger

import (
	"bytes"
	"context"
	"testing"

	dskit_log "github.com/grafana/dskit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// BenchmarkNewOTel measures the allocation overhead of creating a SpanLogger via NewOTel.
// In production this is called on every query and push path through the ingester and distributor.
func BenchmarkNewOTel(b *testing.B) {
	resolver := tenant.NewMultiResolver()

	// Scenario 1: nop logger, no kvps, no tenant — the minimal/common case.
	b.Run("nop_logger/no_kvps/no_tenant", func(b *testing.B) {
		logger := log.NewNopLogger()
		ctx := context.Background()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := NewOTel(ctx, logger, tracer, "test.method", resolver)
			sl.Finish()
		}
	})

	// Scenario 2: nop logger with kvps (debug log at creation).
	b.Run("nop_logger/with_kvps/no_tenant", func(b *testing.B) {
		logger := log.NewNopLogger()
		ctx := context.Background()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := NewOTel(ctx, logger, tracer, "test.method", resolver, "key1", "val1", "key2", "val2")
			sl.Finish()
		}
	})

	// Scenario 3: nop logger, no kvps, with tenant — adds tenant attribute to span.
	b.Run("nop_logger/no_kvps/with_tenant", func(b *testing.B) {
		logger := log.NewNopLogger()
		ctx := user.InjectOrgID(context.Background(), "team-a")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := NewOTel(ctx, logger, tracer, "test.method", resolver)
			sl.Finish()
		}
	})

	// Scenario 4: noDebug logger, no kvps — debug disabled, simulates production logger.
	b.Run("nodebug_logger/no_kvps/no_tenant", func(b *testing.B) {
		logger := noDebugNoopLogger{}
		ctx := context.Background()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := NewOTel(ctx, logger, tracer, "test.method", resolver)
			sl.Finish()
		}
	})

	// Scenario 5: noDebug logger with kvps — debug disabled, kvps skipped.
	b.Run("nodebug_logger/with_kvps/no_tenant", func(b *testing.B) {
		logger := noDebugNoopLogger{}
		ctx := context.Background()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := NewOTel(ctx, logger, tracer, "test.method", resolver, "key1", "val1")
			sl.Finish()
		}
	})

	// Scenario 6: Create + Log — measures NewOTel + one Log call (common production pattern).
	b.Run("nop_logger/create_and_log/no_tenant", func(b *testing.B) {
		logger := log.NewNopLogger()
		ctx := context.Background()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := NewOTel(ctx, logger, tracer, "test.method", resolver)
			_ = sl.Log("msg", "test message", "key", "value")
			sl.Finish()
		}
	})

	// Scenario 7: Create + Log with tenant.
	b.Run("nop_logger/create_and_log/with_tenant", func(b *testing.B) {
		logger := log.NewNopLogger()
		ctx := user.InjectOrgID(context.Background(), "team-a")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := NewOTel(ctx, logger, tracer, "test.method", resolver)
			_ = sl.Log("msg", "test message", "key", "value")
			sl.Finish()
		}
	})

	// Scenario 8: noDebug logger, create + DebugLog (debug skipped, no logging happens).
	b.Run("nodebug_logger/create_and_debuglog/no_tenant", func(b *testing.B) {
		logger := noDebugNoopLogger{}
		ctx := context.Background()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sl, _ := NewOTel(ctx, logger, tracer, "test.method", resolver)
			sl.DebugLog("msg", "test message")
			sl.Finish()
		}
	})

	// Scenario 9: Real logger, create only (simulates production logger setup).
	b.Run("real_logger/create_only/no_tenant", func(b *testing.B) {
		buf := bytes.NewBuffer(nil)
		logger := dskit_log.NewGoKitWithWriter("logfmt", buf)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", Caller(5))
		logger = level.NewFilter(logger, level.AllowInfo())
		logger = loggerWithDebugEnabled{Logger: logger, debugEnabled: false}
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sl, _ := NewOTel(ctx, logger, tracer, "test.method", resolver)
			sl.Finish()
		}
	})

	// Scenario 10: Real logger, create + Log.
	b.Run("real_logger/create_and_log/no_tenant", func(b *testing.B) {
		buf := bytes.NewBuffer(nil)
		logger := dskit_log.NewGoKitWithWriter("logfmt", buf)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", Caller(5))
		logger = level.NewFilter(logger, level.AllowInfo())
		logger = loggerWithDebugEnabled{Logger: logger, debugEnabled: false}
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			sl, _ := NewOTel(ctx, logger, tracer, "test.method", resolver)
			_ = sl.Log("msg", "test message")
			sl.Finish()
		}
	})
}
