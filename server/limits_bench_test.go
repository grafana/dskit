package server

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/tap"
)

// noopMethodLimiter is a minimal GrpcInflightMethodLimiter that does no real work,
// isolating the overhead of TapHandle itself.
type noopMethodLimiter struct{}

func (n noopMethodLimiter) RPCCallStarting(ctx context.Context, _ string, _ metadata.MD) (context.Context, error) {
	return ctx, nil
}

func (n noopMethodLimiter) RPCCallProcessing(_ context.Context, _ string) (func(error), error) {
	return nil, nil
}

func (n noopMethodLimiter) RPCCallFinished(_ context.Context) {}

// BenchmarkTapHandle measures the per-request allocation overhead of TapHandle.
// This is the function identified as a memory allocation hotspot on ingester gRPC paths.
func BenchmarkTapHandle(b *testing.B) {
	logger := log.NewNopLogger()

	b.Run("valid_method", func(b *testing.B) {
		g := newGrpcInflightLimitCheck(noopMethodLimiter{}, logger)
		info := &tap.Info{FullMethodName: "/cortex.Ingester/Push"}
		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resultCtx, err := g.TapHandle(ctx, info)
			if err != nil {
				b.Fatal(err)
			}
			// Simulate what HandleRPC does: retrieve state, stop timer
			if state, ok := resultCtx.Value(gprcInflightLimitCheckerStateKey{}).(*gprcInflightLimitCheckerState); ok {
				state.nonProcessedRequestTimer.Stop()
			}
		}
	})

	b.Run("valid_method_with_metadata", func(b *testing.B) {
		g := newGrpcInflightLimitCheck(noopMethodLimiter{}, logger)
		md := metadata.MD{"content-type": {"application/grpc"}}
		info := &tap.Info{FullMethodName: "/cortex.Ingester/Push", Header: md}
		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resultCtx, err := g.TapHandle(ctx, info)
			if err != nil {
				b.Fatal(err)
			}
			if state, ok := resultCtx.Value(gprcInflightLimitCheckerStateKey{}).(*gprcInflightLimitCheckerState); ok {
				state.nonProcessedRequestTimer.Stop()
			}
		}
	})

	b.Run("invalid_method", func(b *testing.B) {
		g := newGrpcInflightLimitCheck(noopMethodLimiter{}, logger)
		info := &tap.Info{FullMethodName: "bad_method_name"}
		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := g.TapHandle(ctx, info)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("valid_method_parallel", func(b *testing.B) {
		g := newGrpcInflightLimitCheck(noopMethodLimiter{}, logger)
		info := &tap.Info{FullMethodName: "/cortex.Ingester/Push"}

		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			ctx := context.Background()
			for pb.Next() {
				resultCtx, err := g.TapHandle(ctx, info)
				if err != nil {
					b.Fatal(err)
				}
				if state, ok := resultCtx.Value(gprcInflightLimitCheckerStateKey{}).(*gprcInflightLimitCheckerState); ok {
					state.nonProcessedRequestTimer.Stop()
				}
			}
		})
	})

	// Benchmark the full lifecycle: TapHandle -> HandleRPC(InHeader) -> HandleRPC(End)
	b.Run("full_lifecycle", func(b *testing.B) {
		g := newGrpcInflightLimitCheck(noopMethodLimiter{}, logger)
		info := &tap.Info{FullMethodName: "/cortex.Ingester/Push"}
		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resultCtx, err := g.TapHandle(ctx, info)
			if err != nil {
				b.Fatal(err)
			}
			// Simulate InHeader stats event
			g.HandleRPC(resultCtx, &stats.InHeader{})
			// Simulate End stats event
			g.HandleRPC(resultCtx, &stats.End{})
		}
	})

	b.Run("full_lifecycle_parallel", func(b *testing.B) {
		g := newGrpcInflightLimitCheck(noopMethodLimiter{}, logger)
		info := &tap.Info{FullMethodName: "/cortex.Ingester/Push"}

		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			ctx := context.Background()
			for pb.Next() {
				resultCtx, err := g.TapHandle(ctx, info)
				if err != nil {
					b.Fatal(err)
				}
				g.HandleRPC(resultCtx, &stats.InHeader{})
				g.HandleRPC(resultCtx, &stats.End{})
			}
		})
	})
}
