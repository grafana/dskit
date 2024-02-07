package ring

import (
	"context"
	"strings"
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

func TestPartitionRingWatcher_ShouldWatchUpdates(t *testing.T) {
	const ringKey = "ring"

	ctx := context.Background()
	logger := log.NewNopLogger()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	reg := prometheus.NewPedanticRegistry()
	watcher := NewPartitionRingWatcher("test", ringKey, store, logger, reg)

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

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partitions Number of partitions by state in the partitions ring.
		# TYPE partition_ring_partitions gauge
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

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partitions Number of partitions by state in the partitions ring.
		# TYPE partition_ring_partitions gauge
		partition_ring_partitions{name="test",state="Active"} 1
		partition_ring_partitions{name="test",state="Inactive"} 1
	`)))
}
