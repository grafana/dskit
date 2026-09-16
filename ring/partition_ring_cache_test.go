package ring

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var partitionRingCacheTestNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func TestPartitionRing_ShuffleShardCache(t *testing.T) {
	for _, cacheSize := range []int{0, 64} {
		t.Run(fmt.Sprintf("cache_size=%d", cacheSize), func(t *testing.T) {
			for _, lookback := range []bool{false, true} {
				t.Run(fmt.Sprintf("lookback=%t", lookback), func(t *testing.T) {
					t.Run("normalized sizes", func(t *testing.T) {
						for _, tc := range []struct {
							name     string
							states   []PartitionState
							expected []int32
						}{
							{"active", []PartitionState{PartitionActive, PartitionActive, PartitionActive}, []int32{1, 2, 3}},
							{"mixed", []PartitionState{PartitionActive, PartitionActive, PartitionInactive, PartitionPending}, []int32{1, 2}},
							{"empty", nil, []int32{}},
						} {
							t.Run(tc.name, func(t *testing.T) {
								r := newPartitionRingForCacheTest(t, cacheSize, tc.states...)
								expected := tc.expected
								if lookback && tc.name == "mixed" {
									expected = []int32{1, 2, 3}
								}

								first, err := shuffleShardForCacheTest(r, "tenant", r.PartitionsCount(), lookback, partitionRingCacheTestNow)
								require.NoError(t, err)
								require.Equal(t, expected, first.PartitionIDs())

								sizes := []int{math.MinInt, -1, 0, r.PartitionsCount(), r.PartitionsCount() + 1, math.MaxInt}
								for size := r.PartitionsCount() + 2; size < r.PartitionsCount()+34; size++ {
									sizes = append(sizes, size)
								}
								for _, size := range sizes {
									actual, err := shuffleShardForCacheTest(r, "tenant", size, lookback, partitionRingCacheTestNow)
									require.NoError(t, err)
									require.Equal(t, expected, actual.PartitionIDs(), "size %d", size)
									require.Equal(t, 1, partitionRingCacheEntries(r, lookback), "size %d", size)
									require.Same(t, first, actual, "size %d", size)
								}
							})
						}
					})

					t.Run("distinct keys", func(t *testing.T) {
						r := newPartitionRingForCacheTest(t, cacheSize,
							PartitionActive, PartitionActive, PartitionActive, PartitionActive)

						full, err := shuffleShardForCacheTest(r, "tenant", math.MaxInt, lookback, partitionRingCacheTestNow)
						require.NoError(t, err)
						require.Equal(t, []int32{1, 2, 3, 4}, full.PartitionIDs())

						small, err := shuffleShardForCacheTest(r, "tenant", 2, lookback, partitionRingCacheTestNow)
						require.NoError(t, err)
						require.Len(t, small.PartitionIDs(), 2)

						larger, err := shuffleShardForCacheTest(r, "tenant", 3, lookback, partitionRingCacheTestNow)
						require.NoError(t, err)
						require.Len(t, larger.PartitionIDs(), 3)

						other, err := shuffleShardForCacheTest(r, "other", 2, lookback, partitionRingCacheTestNow)
						require.NoError(t, err)
						require.Len(t, other.PartitionIDs(), 2)
						require.NotSame(t, small, other)
						require.Equal(t, 4, partitionRingCacheEntries(r, lookback))

						again, err := shuffleShardForCacheTest(r, "tenant", 2, lookback, partitionRingCacheTestNow)
						require.NoError(t, err)
						require.Same(t, small, again)
					})

					if lookback {
						t.Run("lookback validity", func(t *testing.T) {
							r := newPartitionRingForCacheTest(t, cacheSize,
								PartitionActive, PartitionActive, PartitionInactive, PartitionPending)

							first, err := r.ShuffleShardWithLookback("tenant", r.PartitionsCount()+1, time.Hour, partitionRingCacheTestNow)
							require.NoError(t, err)
							require.Equal(t, []int32{1, 2, 3}, first.PartitionIDs())

							// An inactive partition remains included at the exact lookback boundary.
							boundary, err := r.ShuffleShardWithLookback("tenant", 0, time.Hour, partitionRingCacheTestNow.Add(30*time.Minute))
							require.NoError(t, err)
							require.Equal(t, []int32{1, 2, 3}, boundary.PartitionIDs())
							require.Same(t, first, boundary)

							lateTime := partitionRingCacheTestNow.Add(31 * time.Minute)
							late, err := r.ShuffleShardWithLookback("tenant", math.MaxInt, time.Hour, lateTime)
							require.NoError(t, err)
							require.Equal(t, []int32{1, 2}, late.PartitionIDs())
							require.NotSame(t, first, late)

							// An older request must not reuse or replace the newer cached window.
							earlyAgain, err := r.ShuffleShardWithLookback("tenant", -1, time.Hour, partitionRingCacheTestNow)
							require.NoError(t, err)
							require.Equal(t, []int32{1, 2, 3}, earlyAgain.PartitionIDs())
							lateAgain, err := r.ShuffleShardWithLookback("tenant", r.PartitionsCount(), time.Hour, lateTime)
							require.NoError(t, err)
							require.Same(t, late, lateAgain)
							require.Equal(t, 1, r.shuffleShardCache.cacheWithLookback.len())

							longerLookback, err := r.ShuffleShardWithLookback("tenant", 0, 2*time.Hour, lateTime)
							require.NoError(t, err)
							require.Equal(t, []int32{1, 2, 3}, longerLookback.PartitionIDs())
							require.NotSame(t, late, longerLookback)
							require.Equal(t, 2, r.shuffleShardCache.cacheWithLookback.len())
						})
					}

					t.Run("replacement snapshot", func(t *testing.T) {
						old := newPartitionRingForCacheTest(t, cacheSize, PartitionActive, PartitionActive)
						const requestedSize = 3
						before, err := shuffleShardForCacheTest(old, "tenant", requestedSize, lookback, partitionRingCacheTestNow)
						require.NoError(t, err)
						require.Equal(t, []int32{1, 2}, before.PartitionIDs())

						desc := old.desc.Clone().(*PartitionRingDesc)
						desc.AddPartition(3, PartitionActive, partitionRingCacheTestNow.Add(-2*time.Hour))
						desc.AddPartition(4, PartitionActive, partitionRingCacheTestNow.Add(-2*time.Hour))
						replacement, err := NewPartitionRingWithOptions(*desc, old.opts)
						require.NoError(t, err)

						// The original request becomes a proper subshard in the larger snapshot.
						after, err := shuffleShardForCacheTest(replacement, "tenant", requestedSize, lookback, partitionRingCacheTestNow)
						require.NoError(t, err)
						require.Len(t, after.PartitionIDs(), requestedSize)
						require.NotSame(t, before, after)

						full, err := shuffleShardForCacheTest(replacement, "tenant", 0, lookback, partitionRingCacheTestNow)
						require.NoError(t, err)
						require.Equal(t, []int32{1, 2, 3, 4}, full.PartitionIDs())
						require.Equal(t, 2, partitionRingCacheEntries(replacement, lookback))

						oldAgain, err := shuffleShardForCacheTest(old, "tenant", requestedSize, lookback, partitionRingCacheTestNow)
						require.NoError(t, err)
						require.Same(t, before, oldAgain)
						require.Equal(t, []int32{1, 2}, oldAgain.PartitionIDs())
						require.Equal(t, 1, partitionRingCacheEntries(old, lookback))
					})

					t.Run("concurrent normalized sizes", func(t *testing.T) {
						r := newPartitionRingForCacheTest(t, cacheSize, PartitionActive, PartitionActive)
						const workers = 16
						start := make(chan struct{})
						results := make([]*PartitionRing, workers)
						errs := make([]error, workers)
						var wg sync.WaitGroup
						for i := 0; i < workers; i++ {
							wg.Add(1)
							go func(i int) {
								defer wg.Done()
								<-start
								results[i], errs[i] = shuffleShardForCacheTest(r, "tenant", r.PartitionsCount()+i+1, lookback, partitionRingCacheTestNow)
							}(i)
						}
						close(start)
						wg.Wait()

						for i := range results {
							require.NoError(t, errs[i])
							require.Equal(t, []int32{1, 2}, results[i].PartitionIDs())
						}
						// Concurrent misses may construct different objects, but retain only one entry.
						require.Equal(t, 1, partitionRingCacheEntries(r, lookback))
					})
				})
			}
		})
	}
}

func BenchmarkPartitionRing_ShuffleShardCache(b *testing.B) {
	for _, cacheSize := range []int{0, 64} {
		for _, lookback := range []bool{false, true} {
			b.Run(fmt.Sprintf("cache_size=%d/lookback=%t", cacheSize, lookback), func(b *testing.B) {
				r := newPartitionRingForCacheTest(b, cacheSize,
					PartitionActive, PartitionActive, PartitionActive, PartitionActive,
					PartitionActive, PartitionActive, PartitionActive, PartitionActive)

				b.Run("warm_hit", func(b *testing.B) {
					_, err := shuffleShardForCacheTest(r, "tenant", 3, lookback, partitionRingCacheTestNow)
					require.NoError(b, err)
					b.ReportAllocs()
					for b.Loop() {
						_, err := shuffleShardForCacheTest(r, "tenant", 3, lookback, partitionRingCacheTestNow)
						if err != nil {
							b.Fatal(err)
						}
					}
				})

				b.Run("size_changes", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						// Include identical ring-construction cost in both versions. Each
						// operation is a batch of 32 changes against a fresh eight-partition ring.
						fresh, err := NewPartitionRingWithOptions(r.desc, r.opts)
						if err != nil {
							b.Fatal(err)
						}
						for size := 9; size <= 40; size++ {
							subring, err := shuffleShardForCacheTest(fresh, "tenant", size, lookback, partitionRingCacheTestNow)
							if err != nil {
								b.Fatal(err)
							}
							if subring.PartitionsCount() != 8 {
								b.Fatal("unexpected partition count")
							}
						}
					}
				})
			})
		}
	}
}

func newPartitionRingForCacheTest(t testing.TB, cacheSize int, states ...PartitionState) *PartitionRing {
	t.Helper()
	desc := NewPartitionRingDesc()
	for i, state := range states {
		changedAt := partitionRingCacheTestNow.Add(-2 * time.Hour)
		if state == PartitionInactive {
			changedAt = partitionRingCacheTestNow.Add(-30 * time.Minute)
		}
		desc.AddPartition(int32(i+1), state, changedAt)
	}
	r, err := NewPartitionRingWithOptions(*desc, PartitionRingOptions{ShuffleShardCacheSize: cacheSize})
	require.NoError(t, err)
	return r
}

func shuffleShardForCacheTest(r *PartitionRing, tenant string, size int, lookback bool, now time.Time) (*PartitionRing, error) {
	if lookback {
		return r.ShuffleShardWithLookback(tenant, size, time.Hour, now)
	}
	return r.ShuffleShard(tenant, size)
}

func partitionRingCacheEntries(r *PartitionRing, lookback bool) int {
	if lookback {
		return r.shuffleShardCache.cacheWithLookback.len()
	}
	return r.shuffleShardCache.cacheWithoutLookback.len()
}
