package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

// BenchmarkLRUCache_GetMulti_ConcurrentMissesWithSlowBackend simulates a high rate of concurrent LRU misses
// which fall through to a remote backend with nontrivial latency, e.g. a memcached round trip.
//
// This benchmarks the LRU wrapper locking change in dskit PR #1058.
// With the remote backend cache fetch moved outside the lock, read throughput scales with concurrency.
// Without the narrowed lock scope, all LRU-wrapped cache operations are blocked
// while an LRU read operation fetches missed keys from the remote cache backend.
func BenchmarkLRUCache_GetMulti_ConcurrentMissesWithSlowBackend(b *testing.B) {
	const backendLatency = 100 * time.Microsecond

	backend := NewSlowMockCache(backendLatency)
	reg := prometheus.NewPedanticRegistry()
	lru, err := WrapWithLRUCache(backend, "bench", reg, 100000, time.Minute, log.NewNopLogger())
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	// Atomic counter is used to cheaply create a unique key per call
	// which will always miss the local LRU cache
	// and fall back to the simulated remote backend.
	var counter atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := fmt.Sprintf("key-%d", counter.Add(1))
			lru.GetMulti(ctx, []string{key})
		}
	})
}
