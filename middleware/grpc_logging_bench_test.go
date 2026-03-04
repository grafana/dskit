package middleware

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/dskit/user"
)

// mockServerStream is a minimal grpc.ServerStream for benchmarking.
type mockServerStream struct {
	ctx context.Context
}

func (m *mockServerStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockServerStream) SendHeader(metadata.MD) error { return nil }
func (m *mockServerStream) SetTrailer(metadata.MD)       {}
func (m *mockServerStream) Context() context.Context     { return m.ctx }
func (m *mockServerStream) SendMsg(interface{}) error    { return nil }
func (m *mockServerStream) RecvMsg(interface{}) error    { return nil }

// levelFilteredLogger wraps a go-kit level filter and exposes DebugEnabled()
// so that GRPCServerLog can skip field-building when debug is suppressed.
// This mirrors what production code would use via dskit_log.FilteredLogger.
type levelFilteredLogger struct {
	log.Logger
	debugEnabled bool
}

func (l *levelFilteredLogger) DebugEnabled() bool { return l.debugEnabled }

func newLevelFilteredLogger(next log.Logger, allowDebug bool) *levelFilteredLogger {
	var opt level.Option
	if allowDebug {
		opt = level.AllowDebug()
	} else {
		opt = level.AllowInfo()
	}
	return &levelFilteredLogger{
		Logger:       level.NewFilter(next, opt),
		debugEnabled: allowDebug,
	}
}

// BenchmarkStreamServerInterceptor_NoError_SuccessLogDisabled benchmarks the
// fast-path (no error, success log disabled) — the most common production case.
func BenchmarkStreamServerInterceptor_NoError_SuccessLogDisabled(b *testing.B) {
	logger := level.NewFilter(log.NewNopLogger(), level.AllowError())
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: true}
	ss := &mockServerStream{ctx: context.Background()}
	info := &grpc.StreamServerInfo{FullMethod: "/cortex.Ingester/Push"}
	handler := func(interface{}, grpc.ServerStream) error { return nil }

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = l.StreamServerInterceptor(nil, ss, info, handler)
	}
}

// BenchmarkStreamServerInterceptor_NoError_SuccessLogEnabled_DebugDisabled benchmarks
// the success path when the logger has DebugEnabled()=false (no-op for debug logs).
func BenchmarkStreamServerInterceptor_NoError_SuccessLogEnabled_DebugDisabled(b *testing.B) {
	logger := newLevelFilteredLogger(log.NewNopLogger(), false) // AllowInfo, DebugEnabled=false
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: false}
	ss := &mockServerStream{ctx: context.Background()}
	info := &grpc.StreamServerInfo{FullMethod: "/cortex.Ingester/Push"}
	handler := func(interface{}, grpc.ServerStream) error { return nil }

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = l.StreamServerInterceptor(nil, ss, info, handler)
	}
}

// BenchmarkStreamServerInterceptor_NoError_SuccessLogEnabled_DebugEnabled benchmarks
// the success path when debug is genuinely enabled (the log will be emitted).
func BenchmarkStreamServerInterceptor_NoError_SuccessLogEnabled_DebugEnabled(b *testing.B) {
	logger := newLevelFilteredLogger(log.NewNopLogger(), true) // AllowDebug, DebugEnabled=true
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: false}
	ss := &mockServerStream{ctx: context.Background()}
	info := &grpc.StreamServerInfo{FullMethod: "/cortex.Ingester/Push"}
	handler := func(interface{}, grpc.ServerStream) error { return nil }

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = l.StreamServerInterceptor(nil, ss, info, handler)
	}
}

// BenchmarkStreamServerInterceptor_CanceledError_DebugDisabled benchmarks the
// canceled-error path when debug is suppressed — the dominant production case
// for streaming gRPC (client disconnects are very common).
func BenchmarkStreamServerInterceptor_CanceledError_DebugDisabled(b *testing.B) {
	logger := newLevelFilteredLogger(log.NewNopLogger(), false) // AllowInfo, DebugEnabled=false
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: false}
	ss := &mockServerStream{ctx: context.Background()}
	info := &grpc.StreamServerInfo{FullMethod: "/cortex.Ingester/Push"}
	handler := func(interface{}, grpc.ServerStream) error { return context.Canceled }

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = l.StreamServerInterceptor(nil, ss, info, handler)
	}
}

// BenchmarkStreamServerInterceptor_CanceledError_DebugDisabled_WithOrgID benchmarks
// the canceled-error path with a context carrying an org ID.
func BenchmarkStreamServerInterceptor_CanceledError_DebugDisabled_WithOrgID(b *testing.B) {
	logger := newLevelFilteredLogger(log.NewNopLogger(), false) // AllowInfo, DebugEnabled=false
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: false}
	ctx := user.InjectOrgID(context.Background(), "tenant-1")
	ss := &mockServerStream{ctx: ctx}
	info := &grpc.StreamServerInfo{FullMethod: "/cortex.Ingester/Push"}
	handler := func(interface{}, grpc.ServerStream) error { return context.Canceled }

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = l.StreamServerInterceptor(nil, ss, info, handler)
	}
}

// BenchmarkStreamServerInterceptor_CanceledError (baseline — bare go-kit filter
// without DebugEnabled, falls back to always-true, shows original behaviour).
func BenchmarkStreamServerInterceptor_CanceledError(b *testing.B) {
	logger := level.NewFilter(log.NewNopLogger(), level.AllowInfo())
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: false}
	ss := &mockServerStream{ctx: context.Background()}
	info := &grpc.StreamServerInfo{FullMethod: "/cortex.Ingester/Push"}
	handler := func(interface{}, grpc.ServerStream) error { return context.Canceled }

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = l.StreamServerInterceptor(nil, ss, info, handler)
	}
}

// BenchmarkStreamServerInterceptor_CanceledError_WithOrgID (baseline — bare go-kit
// filter with org ID in context, shows original behaviour).
func BenchmarkStreamServerInterceptor_CanceledError_WithOrgID(b *testing.B) {
	logger := level.NewFilter(log.NewNopLogger(), level.AllowInfo())
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: false}
	ctx := user.InjectOrgID(context.Background(), "tenant-1")
	ss := &mockServerStream{ctx: ctx}
	info := &grpc.StreamServerInfo{FullMethod: "/cortex.Ingester/Push"}
	handler := func(interface{}, grpc.ServerStream) error { return context.Canceled }

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = l.StreamServerInterceptor(nil, ss, info, handler)
	}
}

// BenchmarkUnaryServerInterceptor_NoError_SuccessLogEnabled_DebugDisabled benchmarks
// the success path for unary when debug is suppressed — log work should be skipped.
func BenchmarkUnaryServerInterceptor_NoError_SuccessLogEnabled_DebugDisabled(b *testing.B) {
	logger := newLevelFilteredLogger(log.NewNopLogger(), false) // AllowInfo, DebugEnabled=false
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: false}
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/cortex.Ingester/Push"}
	handler := func(context.Context, interface{}) (interface{}, error) { return nil, nil }

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = l.UnaryServerInterceptor(ctx, nil, info, handler)
	}
}

// BenchmarkUnaryServerInterceptor_NoError_SuccessLogEnabled_DebugEnabled benchmarks
// the success path for unary when debug is genuinely enabled (the log will be emitted).
func BenchmarkUnaryServerInterceptor_NoError_SuccessLogEnabled_DebugEnabled(b *testing.B) {
	logger := newLevelFilteredLogger(log.NewNopLogger(), true) // AllowDebug, DebugEnabled=true
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: false}
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/cortex.Ingester/Push"}
	handler := func(context.Context, interface{}) (interface{}, error) { return nil, nil }

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = l.UnaryServerInterceptor(ctx, nil, info, handler)
	}
}

// BenchmarkUnaryServerInterceptor_CanceledError_DebugDisabled mirrors stream for unary.
func BenchmarkUnaryServerInterceptor_CanceledError_DebugDisabled(b *testing.B) {
	logger := newLevelFilteredLogger(log.NewNopLogger(), false)
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: false}
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/cortex.Ingester/Push"}
	handler := func(context.Context, interface{}) (interface{}, error) {
		return nil, context.Canceled
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = l.UnaryServerInterceptor(ctx, nil, info, handler)
	}
}

// BenchmarkUnaryServerInterceptor_CanceledError_DebugDisabled_WithOrgID mirrors
// the stream benchmark with org ID context.
func BenchmarkUnaryServerInterceptor_CanceledError_DebugDisabled_WithOrgID(b *testing.B) {
	logger := newLevelFilteredLogger(log.NewNopLogger(), false)
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: false}
	ctx := user.InjectOrgID(context.Background(), "tenant-1")
	info := &grpc.UnaryServerInfo{FullMethod: "/cortex.Ingester/Push"}
	handler := func(context.Context, interface{}) (interface{}, error) {
		return nil, context.Canceled
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = l.UnaryServerInterceptor(ctx, nil, info, handler)
	}
}
