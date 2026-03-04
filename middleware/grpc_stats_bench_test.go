package middleware

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/stats"
)

// newBenchStatsHandler builds a handler with real Prometheus vecs (same as prod).
func newBenchStatsHandler(b *testing.B) stats.Handler {
	b.Helper()
	reg := prometheus.NewRegistry()

	received := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bench_received_payload_bytes",
		Buckets: BodySizeBuckets,
	}, []string{"method", "route"})

	sent := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bench_sent_payload_bytes",
		Buckets: BodySizeBuckets,
	}, []string{"method", "route"})

	inflight := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "bench_inflight_requests",
	}, []string{"method", "route"})

	return NewStatsHandler(reg, received, sent, inflight, false)
}

// BenchmarkHandleRPC_InPayload measures the per-message hot path for incoming
// payloads on a streaming RPC (the dominant case in QueryStream).
func BenchmarkHandleRPC_InPayload(b *testing.B) {
	h := newBenchStatsHandler(b)
	const method = "/cortex.Querier/QueryStream"

	// Simulate stream open: TagRPC stores the method name in ctx.
	ctx := h.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: method})

	// Simulate Begin (stream open).
	h.HandleRPC(ctx, &stats.Begin{})

	inPayload := &stats.InPayload{WireLength: 4096}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		h.HandleRPC(ctx, inPayload)
	}
}

// BenchmarkHandleRPC_OutPayload measures the per-message hot path for outgoing
// payloads (server-side streaming responses).
func BenchmarkHandleRPC_OutPayload(b *testing.B) {
	h := newBenchStatsHandler(b)
	const method = "/cortex.Querier/QueryStream"

	ctx := h.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: method})
	h.HandleRPC(ctx, &stats.Begin{})

	outPayload := &stats.OutPayload{WireLength: 4096}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		h.HandleRPC(ctx, outPayload)
	}
}

// BenchmarkHandleRPC_StreamRoundTrip simulates a full streaming round-trip:
// Begin, N×(InPayload + OutPayload), End — matching QueryStream traffic.
func BenchmarkHandleRPC_StreamRoundTrip(b *testing.B) {
	h := newBenchStatsHandler(b)
	const method = "/cortex.Querier/QueryStream"
	const msgsPerStream = 50 // typical number of chunks per query

	ctx := h.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: method})

	inPayload := &stats.InPayload{WireLength: 512}
	outPayload := &stats.OutPayload{WireLength: 8192}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		h.HandleRPC(ctx, &stats.Begin{})
		for j := 0; j < msgsPerStream; j++ {
			h.HandleRPC(ctx, inPayload)
			h.HandleRPC(ctx, outPayload)
		}
		h.HandleRPC(ctx, &stats.End{})
	}
}
