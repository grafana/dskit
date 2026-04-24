package middleware

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/dskit/user"
)

// mockServerStream implements grpc.ServerStream for benchmarking.
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context { return m.ctx }

func BenchmarkStreamServerUserHeaderInterceptor(b *testing.B) {
	// Typical production metadata as seen in Mimir ingester gRPC requests.
	baseMD := metadata.New(map[string]string{
		"authority":            "10.0.0.1:9095",
		"content-type":         "application/grpc",
		"grpc-accept-encoding": "snappy,gzip",
		"uber-trace-id":        "abc123:def456:0:1",
		"user-agent":           "grpc-go/1.61.1",
	})

	noopHandler := func(srv interface{}, stream grpc.ServerStream) error {
		// Simulate what a real handler does: read orgID from context.
		_, err := user.ExtractOrgID(stream.Context())
		return err
	}

	b.Run("typical", func(b *testing.B) {
		md := baseMD.Copy()
		md.Set("x-scope-orgid", "user-1")
		ctx := metadata.NewIncomingContext(context.Background(), md)
		ss := &mockServerStream{ctx: ctx}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := StreamServerUserHeaderInterceptor(nil, ss, nil, noopHandler)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("short_orgid", func(b *testing.B) {
		md := baseMD.Copy()
		md.Set("x-scope-orgid", "t1")
		ctx := metadata.NewIncomingContext(context.Background(), md)
		ss := &mockServerStream{ctx: ctx}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := StreamServerUserHeaderInterceptor(nil, ss, nil, noopHandler)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("long_orgid", func(b *testing.B) {
		md := baseMD.Copy()
		md.Set("x-scope-orgid", "organization-with-a-very-long-tenant-id-1234567890abcdef")
		ctx := metadata.NewIncomingContext(context.Background(), md)
		ss := &mockServerStream{ctx: ctx}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := StreamServerUserHeaderInterceptor(nil, ss, nil, noopHandler)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("many_metadata_keys", func(b *testing.B) {
		md := baseMD.Copy()
		md.Set("x-scope-orgid", "user-1")
		// Add extra metadata to simulate heavier requests
		for i := 0; i < 20; i++ {
			md.Set("x-custom-header-"+string(rune('a'+i)), "value")
		}
		ctx := metadata.NewIncomingContext(context.Background(), md)
		ss := &mockServerStream{ctx: ctx}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := StreamServerUserHeaderInterceptor(nil, ss, nil, noopHandler)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("missing_orgid", func(b *testing.B) {
		md := baseMD.Copy()
		// No x-scope-orgid set
		ctx := metadata.NewIncomingContext(context.Background(), md)
		ss := &mockServerStream{ctx: ctx}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := StreamServerUserHeaderInterceptor(nil, ss, nil, noopHandler)
			if err == nil {
				b.Fatal("expected error")
			}
		}
	})
}

func BenchmarkServerUserHeaderInterceptor(b *testing.B) {
	baseMD := metadata.New(map[string]string{
		"authority":            "10.0.0.1:9095",
		"content-type":         "application/grpc",
		"grpc-accept-encoding": "snappy,gzip",
		"uber-trace-id":        "abc123:def456:0:1",
		"user-agent":           "grpc-go/1.61.1",
	})

	noopHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		_, err := user.ExtractOrgID(ctx)
		return nil, err
	}

	b.Run("typical", func(b *testing.B) {
		md := baseMD.Copy()
		md.Set("x-scope-orgid", "user-1")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := ServerUserHeaderInterceptor(ctx, nil, nil, noopHandler)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

