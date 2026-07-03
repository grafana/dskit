package ring

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/services"
)

func TestPartitionRingWatchers_SingleWatcher(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	writePartitions(t, kvClient, "ring", 0, 1, 2)

	w, err := NewPartitionRingWatchers(NewPartitionRingWatcher("ring", "ring", kvClient, log.NewNopLogger(), nil))
	require.NoError(t, err)
	startPartitionRingWatchers(t, w)

	assert.Equal(t, 1, w.Count())
	assert.Equal(t, 3, w.PartitionRing(0).PartitionsCount())
	assert.Same(t, w.Watcher(0), w.All()[0])
	assert.Same(t, w.Watcher(0).PartitionRing(), w.PartitionRing(0))
}

func TestPartitionRingWatchers_MultipleWatchers(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Each watcher has its own key with a distinct number of partitions.
	writePartitions(t, kvClient, "ring-0", 0)
	writePartitions(t, kvClient, "ring-1", 0, 1)
	writePartitions(t, kvClient, "ring-2", 0, 1, 2)

	w, err := NewPartitionRingWatchers(
		NewPartitionRingWatcher("ring-0", "ring-0", kvClient, log.NewNopLogger(), nil),
		NewPartitionRingWatcher("ring-1", "ring-1", kvClient, log.NewNopLogger(), nil),
		NewPartitionRingWatcher("ring-2", "ring-2", kvClient, log.NewNopLogger(), nil),
	)
	require.NoError(t, err)
	startPartitionRingWatchers(t, w)

	require.Equal(t, 3, w.Count())
	assert.Len(t, w.All(), 3)

	// Each accessor returns the ring at its own index.
	assert.Equal(t, 1, w.PartitionRing(0).PartitionsCount())
	assert.Equal(t, 2, w.PartitionRing(1).PartitionsCount())
	assert.Equal(t, 3, w.PartitionRing(2).PartitionsCount())

	assert.Same(t, w.Watcher(1).PartitionRing(), w.PartitionRing(1))
}

func TestPartitionRingWatchers_DoesNotPanicWithRealRegistererAndDistinctlyNamedWatchers(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// A real registerer panics on duplicate metric registration if the per-watcher ring metrics
	// aren't disambiguated by their (distinct) names.
	reg := prometheus.NewPedanticRegistry()
	var w *PartitionRingWatchers
	require.NotPanics(t, func() {
		var err error
		w, err = NewPartitionRingWatchers(
			NewPartitionRingWatcher("ring-0", "ring-0", kvClient, log.NewNopLogger(), reg),
			NewPartitionRingWatcher("ring-1", "ring-1", kvClient, log.NewNopLogger(), reg),
			NewPartitionRingWatcher("ring-2", "ring-2", kvClient, log.NewNopLogger(), reg),
			NewPartitionRingWatcher("ring-3", "ring-3", kvClient, log.NewNopLogger(), reg),
		)
		require.NoError(t, err)
		require.Equal(t, 4, w.Count())
	})

	// Start and stop so the watcher subservices don't leak goroutines.
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), w))
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), w))
}

func TestPartitionRingWatchers_ReturnsErrorWhenNoWatchersProvided(t *testing.T) {
	w, err := NewPartitionRingWatchers()
	assert.Error(t, err)
	assert.Nil(t, w)
}

// writePartitions stores a partition ring desc with the given partition IDs under the given key.
func writePartitions(t *testing.T, kvClient kv.Client, key string, partitionIDs ...int32) {
	t.Helper()

	require.NoError(t, kvClient.CAS(context.Background(), key, func(interface{}) (interface{}, bool, error) {
		desc := NewPartitionRingDesc()
		for _, id := range partitionIDs {
			desc.AddPartition(id, PartitionActive, time.Now())
		}
		return desc, true, nil
	}))
}

func startPartitionRingWatchers(t *testing.T, w *PartitionRingWatchers) {
	t.Helper()

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), w))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), w))
	})

	// Give the watchers a moment to load the initial ring state from the KV store.
	require.Eventually(t, func() bool {
		return w.PartitionRing(0).PartitionsCount() > 0
	}, time.Second, 10*time.Millisecond)
}
