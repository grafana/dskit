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
		return watcher.PartitionRing().PartitionsCount() == 1 &&
			delegate.PartitionState(1) == PartitionActive // Ensure delegate is updated
	}, time.Second, 10*time.Millisecond)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partition_state_change_locked Whether the partition state change is locked. 1 if locked, 0 if unlocked.
		# TYPE partition_ring_partition_state_change_locked gauge
		partition_ring_partition_state_change_locked{name="test",partition_id="1",state="Active"} 0
		# HELP partition_ring_partition_state_change_locked_timestamp_seconds Unix timestamp (seconds) of when the partition state change lock was last modified. 0 if unlocked.
		# TYPE partition_ring_partition_state_change_locked_timestamp_seconds gauge
		partition_ring_partition_state_change_locked_timestamp_seconds{name="test",partition_id="1",state="Active"} 0
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
		return watcher.PartitionRing().PartitionsCount() == 2 &&
			delegate.PartitionState(2) == PartitionInactive // Ensure delegate is updated
	}, time.Second, 10*time.Millisecond)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partition_state_change_locked Whether the partition state change is locked. 1 if locked, 0 if unlocked.
		# TYPE partition_ring_partition_state_change_locked gauge
		partition_ring_partition_state_change_locked{name="test",partition_id="1",state="Active"} 0
		partition_ring_partition_state_change_locked{name="test",partition_id="2",state="Inactive"} 0
		# HELP partition_ring_partition_state_change_locked_timestamp_seconds Unix timestamp (seconds) of when the partition state change lock was last modified. 0 if unlocked.
		# TYPE partition_ring_partition_state_change_locked_timestamp_seconds gauge
		partition_ring_partition_state_change_locked_timestamp_seconds{name="test",partition_id="1",state="Active"} 0
		partition_ring_partition_state_change_locked_timestamp_seconds{name="test",partition_id="2",state="Inactive"} 0
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
		return watcher.PartitionRing().PartitionsCount() == 3 &&
			delegate.PartitionState(3) == PartitionPending // Ensure delegate is updated
	}, time.Second, 10*time.Millisecond)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partition_state_change_locked Whether the partition state change is locked. 1 if locked, 0 if unlocked.
		# TYPE partition_ring_partition_state_change_locked gauge
		partition_ring_partition_state_change_locked{name="test",partition_id="1",state="Active"} 0
		partition_ring_partition_state_change_locked{name="test",partition_id="2",state="Inactive"} 0
		partition_ring_partition_state_change_locked{name="test",partition_id="3",state="Pending"} 0
		# HELP partition_ring_partition_state_change_locked_timestamp_seconds Unix timestamp (seconds) of when the partition state change lock was last modified. 0 if unlocked.
		# TYPE partition_ring_partition_state_change_locked_timestamp_seconds gauge
		partition_ring_partition_state_change_locked_timestamp_seconds{name="test",partition_id="1",state="Active"} 0
		partition_ring_partition_state_change_locked_timestamp_seconds{name="test",partition_id="2",state="Inactive"} 0
		partition_ring_partition_state_change_locked_timestamp_seconds{name="test",partition_id="3",state="Pending"} 0
		# HELP partition_ring_partitions Number of partitions by state in the partitions ring.
		# TYPE partition_ring_partitions gauge
		partition_ring_partitions{name="test",state="Pending"} 1
		partition_ring_partitions{name="test",state="Active"} 1
		partition_ring_partitions{name="test",state="Inactive"} 1
	`)))

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

func TestPartitionRingWatcher_LockMetrics(t *testing.T) {
	const ringKey = "ring"

	ctx := context.Background()
	logger := log.NewNopLogger()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	reg := prometheus.NewPedanticRegistry()
	watcher := NewPartitionRingWatcher("test", ringKey, store, logger, reg)

	// Start the watcher with an empty ring.
	require.NoError(t, services.StartAndAwaitRunning(ctx, watcher))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, watcher))
	})

	// Add an ACTIVE partition to the ring.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(1, PartitionActive, time.Now())
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		return watcher.PartitionRing().PartitionsCount() == 1
	}, time.Second, 10*time.Millisecond)

	// Lock the partition's state change.
	var lockTimestamp int64
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		now := time.Now()
		lockTimestamp = now.Unix()
		desc.UpdatePartitionStateChangeLock(1, true, now)
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		ring := watcher.PartitionRing()
		partitions := ring.Partitions()
		for _, p := range partitions {
			if p.Id == 1 {
				return p.StateChangeLocked
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)

	// Verify lock metrics show locked state change.
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partition_state_change_locked Whether the partition state change is locked. 1 if locked, 0 if unlocked.
		# TYPE partition_ring_partition_state_change_locked gauge
		partition_ring_partition_state_change_locked{name="test",partition_id="1",state="Active"} 1
	`), "partition_ring_partition_state_change_locked"))

	// Verify lock timestamp is set.
	metrics, err := reg.Gather()
	require.NoError(t, err)
	var foundTimestamp bool
	for _, mf := range metrics {
		if mf.GetName() == "partition_ring_partition_state_change_locked_timestamp_seconds" {
			for _, m := range mf.GetMetric() {
				for _, label := range m.GetLabel() {
					if label.GetName() == "partition_id" && label.GetValue() == "1" {
						assert.Equal(t, float64(lockTimestamp), m.GetGauge().GetValue())
						foundTimestamp = true
					}
				}
			}
		}
	}
	assert.True(t, foundTimestamp, "state change lock timestamp metric should be present")

	// Unlock the partition's state change.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.UpdatePartitionStateChangeLock(1, false, time.Now())
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		ring := watcher.PartitionRing()
		partitions := ring.Partitions()
		for _, p := range partitions {
			if p.Id == 1 {
				return !p.StateChangeLocked
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)

	// Verify state change lock metrics show unlocked state.
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partition_state_change_locked Whether the partition state change is locked. 1 if locked, 0 if unlocked.
		# TYPE partition_ring_partition_state_change_locked gauge
		partition_ring_partition_state_change_locked{name="test",partition_id="1",state="Active"} 0
		# HELP partition_ring_partition_state_change_locked_timestamp_seconds Unix timestamp (seconds) of when the partition state change lock was last modified. 0 if unlocked.
		# TYPE partition_ring_partition_state_change_locked_timestamp_seconds gauge
		partition_ring_partition_state_change_locked_timestamp_seconds{name="test",partition_id="1",state="Active"} 0
		# HELP partition_ring_partitions Number of partitions by state in the partitions ring.
		# TYPE partition_ring_partitions gauge
		partition_ring_partitions{name="test",state="Active"} 1
		partition_ring_partitions{name="test",state="Inactive"} 0
		partition_ring_partitions{name="test",state="Pending"} 0
	`)))
}
