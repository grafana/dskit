package ring

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/services"
)

type ringWatcherDelegateStub struct {
	mu      sync.Mutex
	newRing *PartitionRingDesc
}

func (r *ringWatcherDelegateStub) OnPartitionRingChanged(_, newRing *PartitionRingDesc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.newRing = newRing
}

func (r *ringWatcherDelegateStub) PartitionState(partition int32) PartitionState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.newRing.Partitions[partition].State
}

func TestPartitionRingWatcher_ShouldWatchUpdates(t *testing.T) {
	const ringKey = "ring"

	ctx := context.Background()
	logger := log.NewNopLogger()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	reg := prometheus.NewPedanticRegistry()
	delegate := &ringWatcherDelegateStub{}
	tokens, err := NewPartitionTokenTable(4, logger, nil)
	require.NoError(t, err)
	opts := DefaultPartitionRingOptions()
	opts.TokenGenerator = tokens
	// Set a size so we can assert the options are preserved on update.
	opts.ShuffleShardCacheSize = 1
	watcher := NewPartitionRingWatcherWithOptions("test", ringKey, store, opts, logger, reg).WithDelegate(delegate)

	// PartitionRing should never return nil, even if the watcher hasn't been started yet.
	assert.NotNil(t, watcher.PartitionRing())

	// Start the watcher with an empty ring.
	require.NoError(t, services.StartAndAwaitRunning(ctx, watcher))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, watcher))
	})

	assert.Equal(t, 0, watcher.PartitionRing().PartitionsCount())
	assertPartitionRingWatcherMetrics(t, reg, 0, 0, 0, -1, -1)

	// Add an ACTIVE partition to the ring.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(1, PartitionActive, time.Now())
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		return watcher.PartitionRing().PartitionsCount() == 1 &&
			delegate.PartitionState(1) == PartitionActive // Ensure delegate is updated
	}, time.Second, 10*time.Millisecond)
	// Assert that the options are preserved on update.
	require.Equal(t, opts, watcher.PartitionRing().opts)

	assertPartitionRingWatcherMetrics(t, reg, 0, 1, 0, 1, -1)

	// Add an INACTIVE partition with derived tokens to the ring.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartitionWithDerivedTokens(2, PartitionInactive, time.Now())
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		return watcher.PartitionRing().PartitionsCount() == 2 &&
			delegate.PartitionState(2) == PartitionInactive // Ensure delegate is updated
	}, time.Second, 10*time.Millisecond)

	assertPartitionRingWatcherMetrics(t, reg, 0, 1, 1, 2, 2)
	assert.Empty(t, getPartitionRingFromStore(t, store, ringKey).Partitions[2].Tokens)
	assert.Empty(t, watcher.PartitionRing().desc.Partitions[2].Tokens)
	derivedTokens, err := watcher.PartitionRing().partitionTokens(2)
	require.NoError(t, err)
	assert.Len(t, derivedTokens, optimalTokensPerInstance)

	// Add a PENDING partition to the ring.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(3, PartitionPending, time.Now())
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		return watcher.PartitionRing().PartitionsCount() == 3 &&
			delegate.PartitionState(3) == PartitionPending // Ensure delegate is updated
	}, time.Second, 10*time.Millisecond)

	assertPartitionRingWatcherMetrics(t, reg, 1, 1, 1, 3, 2)

	// Change state of partition to Inactive
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		_, _ = desc.UpdatePartitionState(1, PartitionInactive, time.Now())
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		return watcher.PartitionRing().Partitions()[1].State == PartitionInactive &&
			delegate.PartitionState(1) == PartitionInactive // Ensure delegate is updated
	}, time.Second, 10*time.Millisecond)
}

func assertPartitionRingWatcherMetrics(t *testing.T, reg prometheus.Gatherer, pending, active, inactive, maxID, maxDerivedID int) {
	t.Helper()
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP partition_ring_partitions Number of partitions by state in the partitions ring.
		# TYPE partition_ring_partitions gauge
		partition_ring_partitions{name="test",state="Pending"} %d
		partition_ring_partitions{name="test",state="Active"} %d
		partition_ring_partitions{name="test",state="Inactive"} %d
		# HELP partition_ring_max_partition_id Highest partition ID in the ring, or -1 when empty.
		# TYPE partition_ring_max_partition_id gauge
		partition_ring_max_partition_id{name="test"} %d
		# HELP partition_ring_max_derived_partition_id Highest partition ID using derived tokens in the ring, or -1 when none.
		# TYPE partition_ring_max_derived_partition_id gauge
		partition_ring_max_derived_partition_id{name="test"} %d
	`, pending, active, inactive, maxID, maxDerivedID))))
}

// Without a token generator, a derived partition cannot be resolved.
func TestPartitionRingWatcher_FailsToStartWhenTokensCannotBeResolved(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	derived := NewPartitionRingDesc()
	derived.AddPartitionWithDerivedTokens(42, PartitionActive, time.Now())
	setPartitionRingDesc(t, store, derived)

	watcher := NewPartitionRingWatcherWithOptions("test", ringKey, store, DefaultPartitionRingOptions(), log.NewNopLogger(), nil)
	require.Error(t, services.StartAndAwaitRunning(ctx, watcher))
	require.Equal(t, services.Failed, watcher.State())

	assert.Empty(t, watcher.PartitionRing().PartitionIDs())
	assert.Equal(t, float64(-1), testutil.ToFloat64(watcher.maxPartitionIDGauge))
	assert.Equal(t, float64(-1), testutil.ToFloat64(watcher.maxDerivedPartitionIDGauge))
}

// When a later update cannot be resolved, the watcher fails. PartitionRing still returns the last
// ring it built until the application stops.
func TestPartitionRingWatcher_StopsWhenUpdatedTokensCannotBeResolved(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	stored := NewPartitionRingDesc()
	stored.AddPartition(3, PartitionActive, time.Now())
	setPartitionRingDesc(t, store, stored)

	watcher := NewPartitionRingWatcherWithOptions("test", ringKey, store, DefaultPartitionRingOptions(), log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, watcher))
	t.Cleanup(watcher.StopAsync)

	derived := NewPartitionRingDesc()
	derived.AddPartitionWithDerivedTokens(42, PartitionActive, time.Now())
	setPartitionRingDesc(t, store, derived)
	require.Error(t, watcher.AwaitTerminated(ctx))
	require.Equal(t, services.Failed, watcher.State())

	assert.Equal(t, []int32{3}, watcher.PartitionRing().PartitionIDs())
	assert.Equal(t, float64(3), testutil.ToFloat64(watcher.maxPartitionIDGauge))
	assert.Equal(t, float64(-1), testutil.ToFloat64(watcher.maxDerivedPartitionIDGauge))
}

func setPartitionRingDesc(t *testing.T, store kv.Client, desc *PartitionRingDesc) {
	t.Helper()
	require.NoError(t, store.CAS(context.Background(), ringKey, func(interface{}) (interface{}, bool, error) {
		return desc, true, nil
	}))
}

// partitionCreationDescs returns a ring of active partitions and the descriptors that follow it
// while k partitions are created the way a scale-up creates them: each is added Pending, its two
// owners register, then it switches to Active. Steps are batched across partitions, as when many
// ingesters start at once, so 4k descriptors follow the first.
func partitionCreationDescs(tb testing.TB, active, k int) []*PartitionRingDesc {
	tokens, err := generatePartitionTokens(int32(active + k - 1))
	require.NoError(tb, err)
	start := NewPartitionRingDesc()
	for id := int32(0); id < int32(active); id++ {
		start.Partitions[id] = PartitionDesc{Id: id, State: PartitionActive, StateTimestamp: 10, Tokens: tokens[id]}
		for _, zone := range []string{"a", "b"} {
			start.Owners[fmt.Sprintf("ingester-zone-%s-%d", zone, id)] = OwnerDesc{OwnedPartition: id, State: OwnerActive}
		}
	}
	descs := []*PartitionRingDesc{start}
	step := func(change func(d *PartitionRingDesc)) {
		next := descs[len(descs)-1].Clone().(*PartitionRingDesc)
		change(next)
		descs = append(descs, next)
	}
	created := make([]int32, k)
	for j := range created {
		created[j] = int32(active + j)
	}
	for _, id := range created {
		step(func(d *PartitionRingDesc) {
			d.Partitions[id] = PartitionDesc{Id: id, State: PartitionPending, StateTimestamp: 20, Tokens: tokens[id]}
		})
	}
	for _, zone := range []string{"a", "b"} {
		for _, id := range created {
			step(func(d *PartitionRingDesc) {
				d.Owners[fmt.Sprintf("ingester-zone-%s-%d", zone, id)] = OwnerDesc{OwnedPartition: id, State: OwnerActive}
			})
		}
	}
	for _, id := range created {
		step(func(d *PartitionRingDesc) {
			p := d.Partitions[id]
			p.State, p.StateTimestamp = PartitionActive, 30
			d.Partitions[id] = p
		})
	}
	return descs
}

// TestPartitionRingWatcher_ShardsThroughPartitionCreation checks that shards served while partitions
// are created match shards of a freshly built ring after every update, and that updates touching only
// Pending partitions and their owners keep serving the previously built shards.
func TestPartitionRingWatcher_ShardsThroughPartitionCreation(t *testing.T) {
	// Sizes cover the default (0, every partition), small shards, and a shard of a quarter of the
	// ring or more, which is filtered from the parent ring rather than built from its own tokens.
	sizes := []int{0, 1, 5, 12}
	descs := partitionCreationDescs(t, 40, 4)
	watcher := NewPartitionRingWatcherWithOptions("test", "test", nil, DefaultPartitionRingOptions(), log.NewNopLogger(), nil)
	now := time.Unix(1000, 0)
	var prev []*PartitionRing
	for i, desc := range descs {
		require.NoError(t, watcher.updatePartitionRing(desc))
		fresh, err := NewPartitionRingWithOptions(*desc, DefaultPartitionRingOptions())
		require.NoError(t, err)
		pendingOnly := i > 0 && sameExceptPending(descs[i-1], desc)

		shards := make([]*PartitionRing, len(sizes))
		for j, size := range sizes {
			tenant := fmt.Sprintf("tenant-%d", j)
			got, err := watcher.PartitionRing().ShuffleShard(tenant, size)
			require.NoError(t, err)
			want, err := fresh.ShuffleShard(tenant, size)
			require.NoError(t, err)
			require.Equal(t, want.PartitionIDs(), got.PartitionIDs(), "update %d, size %d", i, size)
			if pendingOnly {
				require.Same(t, prev[j], got, "update %d, size %d: shard rebuilt after a Pending-only update", i, size)
			}
			shards[j] = got

			gotLookback, err := watcher.PartitionRing().ShuffleShardWithLookback(tenant, size, time.Hour, now)
			require.NoError(t, err)
			wantLookback, err := fresh.ShuffleShardWithLookback(tenant, size, time.Hour, now)
			require.NoError(t, err)
			require.Equal(t, wantLookback.PartitionIDs(), gotLookback.PartitionIDs(), "update %d, size %d, with lookback", i, size)
		}
		prev = shards
	}
}

// BenchmarkPartitionRingWatcher_PartitionCreation times what a distributor spends while 8 partitions
// are created: for each ring update, building the new ring, then serving the write shard of every
// tenant, as if each pushed before the next update. Most tenants use the default shard size, as in a
// multi-tenant cluster; tenants=0 times the ring builds alone.
func BenchmarkPartitionRingWatcher_PartitionCreation(b *testing.B) {
	const created = 8
	var tenants []string
	var sizes []int
	for i := 0; i < 100; i++ {
		tenants, sizes = append(tenants, fmt.Sprintf("default-%d", i)), append(sizes, 0)
	}
	for _, size := range []int{2, 4, 8, 16, 32, 64, 128, 309, 674, 1022} {
		tenants, sizes = append(tenants, fmt.Sprintf("explicit-%d", size)), append(sizes, size)
	}
	for _, active := range []int{500, 1500} {
		descs := partitionCreationDescs(b, active, created)
		for _, n := range []int{0, len(tenants)} {
			b.Run(fmt.Sprintf("partitions=%d/tenants=%d", active, n), func(b *testing.B) {
				watcher := NewPartitionRingWatcherWithOptions("test", "test", nil, DefaultPartitionRingOptions(), log.NewNopLogger(), nil)
				serve := func() {
					for j := 0; j < n; j++ {
						if _, err := watcher.PartitionRing().ShuffleShard(tenants[j], sizes[j]); err != nil {
							b.Fatal(err)
						}
					}
				}
				for i := 0; i < b.N; i++ {
					// Start from a ring whose shards are all cached, as in steady state.
					b.StopTimer()
					require.NoError(b, watcher.updatePartitionRing(descs[0]))
					serve()
					b.StartTimer()
					for _, desc := range descs[1:] {
						if err := watcher.updatePartitionRing(desc); err != nil {
							b.Fatal(err)
						}
						serve()
					}
				}
				b.ReportMetric(float64(b.Elapsed().Milliseconds())/float64(b.N)/created, "ms/partition")
			})
		}
	}
}
