package middleware

import (
	"context"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/logging"
)

func BenchmarkGRPCServerLog_UnaryServerInterceptor_NoError(b *testing.B) {
	logger := logging.GoKit(level.NewFilter(log.NewNopLogger(), level.AllowError()))
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: true}
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "Test"}

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	}

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, _ = l.UnaryServerInterceptor(ctx, nil, info, handler)
	}
}
