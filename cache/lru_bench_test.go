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

// delayedMockCache wraps MockCache and sleeps for a fixed delay on every
// GetMultiWithError call, standing in for a network round trip to a real
// backend such as memcached.
type delayedMockCache struct {
	*MockCache
	delay time.Duration
}

func newDelayedMockCache(delay time.Duration) *delayedMockCache {
	return &delayedMockCache{
		MockCache: NewMockCache(),
		delay:     delay,
	}
}

func (d *delayedMockCache) GetMulti(ctx context.Context, keys []string, opts ...Option) map[string][]byte {
	result, _ := d.GetMultiWithError(ctx, keys, opts...)
	return result
}

func (d *delayedMockCache) GetMultiWithError(ctx context.Context, keys []string, opts ...Option) (map[string][]byte, error) {
	time.Sleep(d.delay)
	return d.MockCache.GetMultiWithError(ctx, keys, opts...)
}

// BenchmarkLRUCache_GetMulti_ConcurrentMissesWithSlowBackend simulates concurrent
// callers that all miss the LRU and fall through to a backend with a fixed
// per-call latency, similar to a memcached round trip.
//
// Run with varying -cpu values (e.g. -cpu 1,4,16) to compare throughput across
// concurrency levels. With the backend fetch outside the lock, throughput
// scales with concurrency; if the fetch were still under the lock, calls would
// serialize behind it and throughput would stay flat regardless of -cpu.
func BenchmarkLRUCache_GetMulti_ConcurrentMissesWithSlowBackend(b *testing.B) {
	const backendLatency = 200 * time.Microsecond

	backend := newDelayedMockCache(backendLatency)
	reg := prometheus.NewPedanticRegistry()
	lru, err := WrapWithLRUCache(backend, "bench", reg, 100000, time.Minute, log.NewNopLogger())
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	var counter atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// A unique key per call guarantees every request misses the LRU
			// and falls through to the delayed backend.
			key := fmt.Sprintf("key-%d", counter.Add(1))
			lru.GetMulti(ctx, []string{key})
		}
	})
}
