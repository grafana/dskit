package ring

import (
	"context"
	"sync"
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
	const eventuallyTick = 10 * time.Millisecond

	ctx := context.Background()
	logger := log.NewNopLogger()

	t.Run("should wait for the configured minimum number of owners before switching a pending partition to active", func(t *testing.T) {
		t.Parallel()

		lifecycler1aConfig := createTestPartitionInstanceLifecyclerConfig(1, "instance-zone-a-1")
		lifecycler1bConfig := createTestPartitionInstanceLifecyclerConfig(1, "instance-zone-b-1")
		for _, cfg := range []*PartitionInstanceLifecyclerConfig{&lifecycler1aConfig, &lifecycler1bConfig} {
			cfg.WaitOwnersCountOnPending = 2
			cfg.WaitOwnersDurationOnPending = 0
		}

		store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })

		// Start instance-zone-a-1 lifecycler.
		lifecycler1a := NewPartitionInstanceLifecycler(lifecycler1aConfig, "test", ringKey, store, logger, nil)
		require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler1a))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler1a))
		})

		// We expect the partition to NOT switch to active.
		time.Sleep(2 * lifecycler1aConfig.PollingInterval)

		actual := getPartitionRingFromStore(t, store, ringKey)
		assert.Len(t, actual.Partitions, 1)
		assert.True(t, actual.HasPartition(1))
		assert.Equal(t, PartitionPending, actual.Partitions[1].State)
		assert.ElementsMatch(t, []string{"instance-zone-a-1"}, actual.ownersByPartition()[1])

		// Start instance-zone-b-1 lifecycler.
		lifecycler1b := NewPartitionInstanceLifecycler(lifecycler1bConfig, "test", ringKey, store, logger, nil)
		require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler1b))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler1b))
		})

		actual = getPartitionRingFromStore(t, store, ringKey)
		assert.ElementsMatch(t, []string{"instance-zone-a-1", "instance-zone-b-1"}, actual.ownersByPartition()[1])

		// We expect the partition to switch to active state.
		assert.Eventually(t, func() bool {
			actual := getPartitionRingFromStore(t, store, ringKey)
			return actual.Partitions[1].State == PartitionActive
		}, time.Second, eventuallyTick)
	})

	t.Run("should wait for the configured minimum waiting time before switching a pending partition to active", func(t *testing.T) {
		t.Parallel()

		lifecyclerConfig := createTestPartitionInstanceLifecyclerConfig(1, "instance-1")
		lifecyclerConfig.WaitOwnersCountOnPending = 1
		lifecyclerConfig.WaitOwnersDurationOnPending = 2 * time.Second

		store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })

		// Start lifecycler.
		startTime := time.Now()
		lifecycler := NewPartitionInstanceLifecycler(lifecyclerConfig, "test", ringKey, store, logger, nil)
		require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
		})

		actual := getPartitionRingFromStore(t, store, ringKey)
		assert.Len(t, actual.Partitions, 1)
		assert.True(t, actual.HasPartition(1))
		assert.Equal(t, PartitionPending, actual.Partitions[1].State)
		assert.ElementsMatch(t, []string{"instance-1"}, actual.ownersByPartition()[1])

		// Wait until active.
		assert.Eventually(t, func() bool {
			actual := getPartitionRingFromStore(t, store, ringKey)
			return actual.Partitions[1].State == PartitionActive
		}, 2*lifecyclerConfig.WaitOwnersDurationOnPending, eventuallyTick)

		// The partition should have been switch from pending to active after the minimum waiting period.
		assert.GreaterOrEqual(t, time.Since(startTime), lifecyclerConfig.WaitOwnersDurationOnPending)
	})

	t.Run("inactive partitions should be removed only if the waiting period passed and there are no owners", func(t *testing.T) {
		t.Parallel()

		lifecycler1aConfig := createTestPartitionInstanceLifecyclerConfig(1, "instance-zone-a-1")
		lifecycler1bConfig := createTestPartitionInstanceLifecyclerConfig(1, "instance-zone-b-1")
		lifecycler2aConfig := createTestPartitionInstanceLifecyclerConfig(2, "instance-zone-a-2")
		for _, cfg := range []*PartitionInstanceLifecyclerConfig{&lifecycler1aConfig, &lifecycler1bConfig, &lifecycler2aConfig} {
			cfg.DeleteInactivePartitionAfterDuration = 100 * time.Millisecond
		}

		store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })

		// Start all lifecyclers.
		lifecycler1a := NewPartitionInstanceLifecycler(lifecycler1aConfig, "test", ringKey, store, logger, nil)
		lifecycler1b := NewPartitionInstanceLifecycler(lifecycler1bConfig, "test", ringKey, store, logger, nil)
		lifecycler2a := NewPartitionInstanceLifecycler(lifecycler2aConfig, "test", ringKey, store, logger, nil)
		require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler1a))
		require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler1b))
		require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler2a))
		t.Cleanup(func() {
			// Ensure we stop all lifecyclers once the test is terminated.
			require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler1a))
			require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler1b))
			require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler2a))
		})

		actual := getPartitionRingFromStore(t, store, ringKey)
		assert.Len(t, actual.Partitions, 2)
		assert.True(t, actual.HasPartition(1))
		assert.True(t, actual.HasPartition(2))
		assert.Equal(t, map[int32][]string{1: {"instance-zone-a-1", "instance-zone-b-1"}, 2: {"instance-zone-a-2"}}, actual.ownersByPartition())

		// Switch partition 1 to inactive.
		require.NoError(t, lifecycler1a.ChangePartitionState(ctx, PartitionInactive))

		// Wait longer than deletion wait period. We expect that the partition is not
		// delete yet because there are still owners.
		time.Sleep(2 * lifecycler1aConfig.DeleteInactivePartitionAfterDuration)

		actual = getPartitionRingFromStore(t, store, ringKey)
		assert.Len(t, actual.Partitions, 2)
		assert.True(t, actual.HasPartition(1))
		assert.True(t, actual.HasPartition(2))
		assert.Equal(t, map[int32][]string{1: {"instance-zone-a-1", "instance-zone-b-1"}, 2: {"instance-zone-a-2"}}, actual.ownersByPartition())

		// Stop instance-zone-a-1.
		lifecycler1a.SetRemoveOwnerOnShutdown(true)
		require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler1a))

		actual = getPartitionRingFromStore(t, store, ringKey)
		assert.Len(t, actual.Partitions, 2)
		assert.True(t, actual.HasPartition(1))
		assert.True(t, actual.HasPartition(2))
		assert.Equal(t, map[int32][]string{1: {"instance-zone-b-1"}, 2: {"instance-zone-a-2"}}, actual.ownersByPartition())

		// Stop instance-zone-b-1.
		lifecycler1b.SetRemoveOwnerOnShutdown(true)
		require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler1b))

		actual = getPartitionRingFromStore(t, store, ringKey)
		assert.Equal(t, map[int32][]string{2: {"instance-zone-a-2"}}, actual.ownersByPartition())

		// We expect remaining lifecyclers to clean up the inactive partition now.
		assert.Eventually(t, func() bool {
			actual := getPartitionRingFromStore(t, store, ringKey)
			return !actual.HasPartition(1)
		}, time.Second, eventuallyTick)

		// Partition 2 should still exist.
		actual = getPartitionRingFromStore(t, store, ringKey)
		assert.True(t, actual.HasPartition(2))
	})

	t.Run("should not create the partition but wait until partition exists in the ring if lifecycler has been configured to not create partition at startup", func(t *testing.T) {
		t.Parallel()

		cfg := createTestPartitionInstanceLifecyclerConfig(1, "instance-1")

		store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })

		// Create the lifecycler.
		lifecycler := NewPartitionInstanceLifecycler(cfg, "test", ringKey, store, logger, nil)
		lifecycler.SetCreatePartitionOnStartup(false)

		// Start the lifecycler (will block until the partition is created).
		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			defer wg.Done()
			require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
			})
		}()

		// No matter how long we wait, we expect the lifecycler hasn't been
		// started yet and the partition was not created.
		time.Sleep(10 * cfg.PollingInterval)

		assert.Equal(t, services.Starting, lifecycler.State())
		actual := getPartitionRingFromStore(t, store, ringKey)
		assert.False(t, actual.HasPartition(1))
		assert.Equal(t, map[int32][]string{}, actual.ownersByPartition())

		// Create the partition.
		require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
			ring := GetOrCreatePartitionRingDesc(in)
			ring.AddPartition(1, PartitionPending, time.Now())
			return ring, true, nil
		}))

		// The partition has been created, so we expect the lifecycler to complete the startup.
		wg.Wait()
		require.Equal(t, services.Running, lifecycler.State())

		actual = getPartitionRingFromStore(t, store, ringKey)
		assert.True(t, actual.HasPartition(1))
		assert.Equal(t, map[int32][]string{1: {"instance-1"}}, actual.ownersByPartition())
	})

	t.Run("should stop waiting for partition creation if the context gets canceled", func(t *testing.T) {
		t.Parallel()

		cfg := createTestPartitionInstanceLifecyclerConfig(1, "instance-1")

		store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })

		// Create the lifecycler.
		lifecycler := NewPartitionInstanceLifecycler(cfg, "test", ringKey, store, logger, nil)
		lifecycler.SetCreatePartitionOnStartup(false)

		// Start the lifecycler (will block until the partition is created).
		startCtx, cancelStart := context.WithCancel(ctx)
		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			defer wg.Done()

			err := services.StartAndAwaitRunning(startCtx, lifecycler)
			require.ErrorIs(t, err, context.Canceled)
		}()

		// No matter how long we wait, we expect the lifecycler hasn't been
		// started yet and the partition was not created.
		time.Sleep(10 * cfg.PollingInterval)

		assert.Equal(t, services.Starting, lifecycler.State())
		actual := getPartitionRingFromStore(t, store, ringKey)
		assert.False(t, actual.HasPartition(1))
		assert.Equal(t, map[int32][]string{}, actual.ownersByPartition())

		// We expect the service starting to interrupt once we cancel the context.
		cancelStart()
		wg.Wait()

		actual = getPartitionRingFromStore(t, store, ringKey)
		assert.False(t, actual.HasPartition(1))
		assert.Equal(t, map[int32][]string{}, actual.ownersByPartition())

		assert.Eventually(t, func() bool {
			return lifecycler.State() == services.Failed
		}, time.Second, eventuallyTick)
	})
}

func TestPartitionInstanceLifecycler_GetAndChangePartitionState(t *testing.T) {
	ctx := context.Background()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Start lifecycler.
	cfg := createTestPartitionInstanceLifecyclerConfig(1, "instance-1")
	lifecycler := NewPartitionInstanceLifecycler(cfg, "test", ringKey, store, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
	})

	assertPartitionState := func(expected PartitionState) {
		actualState, _, err := lifecycler.GetPartitionState(ctx)
		require.NoError(t, err)
		assert.Equal(t, expected, actualState)

		actualRing := getPartitionRingFromStore(t, store, ringKey)
		assert.Equal(t, expected, actualRing.Partitions[1].State)
	}

	// Wait until active.
	assert.Eventually(t, func() bool {
		actual := getPartitionRingFromStore(t, store, ringKey)
		return actual.Partitions[1].State == PartitionActive
	}, time.Second, 10*time.Millisecond)

	actualState, _, err := lifecycler.GetPartitionState(ctx)
	require.NoError(t, err)
	assert.Equal(t, PartitionActive, actualState)

	// A request to switch to state whose transition is not allowed should return error.
	require.ErrorIs(t, lifecycler.ChangePartitionState(ctx, PartitionPending), ErrPartitionStateChangeNotAllowed)
	assertPartitionState(PartitionActive)

	// Switch to inactive.
	require.NoError(t, lifecycler.ChangePartitionState(ctx, PartitionInactive))
	assertPartitionState(PartitionInactive)

	// A request to switch to the same state should be a no-op.
	require.NoError(t, lifecycler.ChangePartitionState(ctx, PartitionInactive))
	assertPartitionState(PartitionInactive)

	// Should NOT allow changing from inactive to pending. The reason is that due to async ring changes propagation
	// (via memberlist) there's no guarantee that the partition was already switched from inactive to active in the
	// meanwhile by another instance, and the switch from active to pending is not allowed in order to guarantee
	// read consistency.
	require.ErrorIs(t, lifecycler.ChangePartitionState(ctx, PartitionPending), ErrPartitionStateChangeNotAllowed)
	assertPartitionState(PartitionInactive)
}

func getPartitionRingFromStore(t *testing.T, store kv.Client, key string) *PartitionRingDesc {
	in, err := store.Get(context.Background(), key)
	require.NoError(t, err)

	return GetOrCreatePartitionRingDesc(in)
}

func getPartitionStateFromStore(t *testing.T, store kv.Client, key string, partitionID int32) PartitionState {
	partition, ok := getPartitionRingFromStore(t, store, key).GetPartitions()[partitionID]
	if !ok {
		return PartitionUnknown
	}
	return partition.State
}

func createTestPartitionInstanceLifecyclerConfig(partitionID int32, instanceID string) PartitionInstanceLifecyclerConfig {
	return PartitionInstanceLifecyclerConfig{
		PartitionID:                          partitionID,
		InstanceID:                           instanceID,
		WaitOwnersCountOnPending:             0,
		WaitOwnersDurationOnPending:          0,
		DeleteInactivePartitionAfterDuration: 0,
		PollingInterval:                      10 * time.Millisecond,
	}
}
