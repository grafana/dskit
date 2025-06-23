package ring

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	watcher := NewPartitionRingWatcher("test", ringKey, store, logger, reg).WithDelegate(delegate)

	// PartitionRing should never return nil, even if the watcher hasn't been started yet.
	assert.NotNil(t, watcher.PartitionRing())

	// Start the watcher with an empty ring.
	require.NoError(t, services.StartAndAwaitRunning(ctx, watcher))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, watcher))
	})

	assert.Equal(t, 0, watcher.PartitionRing().PartitionsCount())
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partitions Number of partitions by state in the partitions ring.
		# TYPE partition_ring_partitions gauge
		partition_ring_partitions{name="test",state="Pending"} 0
		partition_ring_partitions{name="test",state="Active"} 0
		partition_ring_partitions{name="test",state="Inactive"} 0
	`)))

	// Add an ACTIVE partition to the ring.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(1, PartitionActive, time.Now())
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		return watcher.PartitionRing().PartitionsCount() == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, PartitionActive, delegate.PartitionState(1))

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partitions Number of partitions by state in the partitions ring.
		# TYPE partition_ring_partitions gauge
		partition_ring_partitions{name="test",state="Pending"} 0
		partition_ring_partitions{name="test",state="Active"} 1
		partition_ring_partitions{name="test",state="Inactive"} 0
	`)))

	// Add an INACTIVE partition to the ring.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(2, PartitionInactive, time.Now())
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		return watcher.PartitionRing().PartitionsCount() == 2
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, PartitionInactive, delegate.PartitionState(2))

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partitions Number of partitions by state in the partitions ring.
		# TYPE partition_ring_partitions gauge
		partition_ring_partitions{name="test",state="Pending"} 0
		partition_ring_partitions{name="test",state="Active"} 1
		partition_ring_partitions{name="test",state="Inactive"} 1
	`)))

	// Add a PENDING partition to the ring.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(3, PartitionPending, time.Now())
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		return watcher.PartitionRing().PartitionsCount() == 3
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, PartitionPending, delegate.PartitionState(3))

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partitions Number of partitions by state in the partitions ring.
		# TYPE partition_ring_partitions gauge
		partition_ring_partitions{name="test",state="Pending"} 1
		partition_ring_partitions{name="test",state="Active"} 1
		partition_ring_partitions{name="test",state="Inactive"} 1
	`)))

	// Change state of partition to Inactive
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.UpdatePartitionState(1, PartitionInactive, time.Now())
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		return watcher.PartitionRing().Partitions()[1].State == PartitionInactive
	}, time.Second, 10*time.Millisecond)

	// Ensure delegate is called
	require.Equal(t, PartitionInactive, delegate.PartitionState(1))
}
