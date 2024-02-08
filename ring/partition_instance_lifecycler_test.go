package ring

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/services"
)

func TestPartitionInstanceLifecycler(t *testing.T) {
	ctx := context.Background()
	ringKey := "test"
	logger := log.NewLogfmtLogger(os.Stderr)
	lifecyclerConfig1 := PartitionInstanceLifecyclerConfig{
		PartitionID:                          1,
		InstanceID:                           "instance-1",
		WaitOwnersCountOnPending:             2,
		WaitOwnersDurationOnPending:          0,
		DeleteInactivePartitionAfterDuration: 0,
		reconcileInterval:                    100 * time.Millisecond,
	}
	lifecyclerConfig2 := lifecyclerConfig1
	lifecyclerConfig2.InstanceID = "instance-2"

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Start instance-1 lifecycler.
	lifecycler1 := NewPartitionInstanceLifecycler(lifecyclerConfig1, "test", ringKey, store, logger)
	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler1))

	actual := getPartitionRingFromStore(t, store, ringKey)
	assert.Len(t, actual.Partitions, 1)
	assert.True(t, actual.HasPartition(1))
	assert.Equal(t, PartitionPending, actual.Partitions[1].State)
	assert.ElementsMatch(t, []string{"instance-1"}, actual.ownersByPartition()[1])

	// Start instance-2 lifecycler.
	lifecycler2 := NewPartitionInstanceLifecycler(lifecyclerConfig2, "test", ringKey, store, logger)
	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler2))

	actual = getPartitionRingFromStore(t, store, ringKey)
	assert.Len(t, actual.Partitions, 1)
	assert.True(t, actual.HasPartition(1))
	assert.ElementsMatch(t, []string{"instance-1", "instance-2"}, actual.ownersByPartition()[1])

	// We expect the partition to switch to active state.
	assert.Eventually(t, func() bool {
		actual := getPartitionRingFromStore(t, store, ringKey)
		return actual.Partitions[1].State == PartitionActive
	}, time.Second, 100*time.Millisecond)

	// Stop instance-1 lifecycler.
	require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler1))

	actual = getPartitionRingFromStore(t, store, ringKey)
	assert.Len(t, actual.Partitions, 1)
	assert.True(t, actual.HasPartition(1))
	assert.ElementsMatch(t, []string{"instance-1", "instance-2"}, actual.ownersByPartition()[1])

	// Start and stop again instance-1, but this time enable owner removal on shutdown.
	lifecycler1 = NewPartitionInstanceLifecycler(lifecyclerConfig1, "test", ringKey, store, logger)
	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler1))
	lifecycler1.SetRemoveOwnerOnShutdown(true)
	require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler1))

	actual = getPartitionRingFromStore(t, store, ringKey)
	assert.Len(t, actual.Partitions, 1)
	assert.True(t, actual.HasPartition(1))
	assert.ElementsMatch(t, []string{"instance-2"}, actual.ownersByPartition()[1])

	// Stop instance-2 lifecycler.
	require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler2))

	actual = getPartitionRingFromStore(t, store, ringKey)
	assert.Len(t, actual.Partitions, 1)
	assert.True(t, actual.HasPartition(1))
	assert.ElementsMatch(t, []string{"instance-2"}, actual.ownersByPartition()[1])
}

func getPartitionRingFromStore(t *testing.T, store kv.Client, key string) *PartitionRingDesc {
	in, err := store.Get(context.Background(), key)
	require.NoError(t, err)

	return GetOrCreatePartitionRingDesc(in)
}
