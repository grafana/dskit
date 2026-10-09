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
