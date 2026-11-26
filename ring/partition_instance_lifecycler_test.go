package ring

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/kv/memberlist"
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
		lifecycler1a, err := NewPartitionInstanceLifecycler(lifecycler1aConfig, "test", ringKey, store, logger, nil)
		require.NoError(t, err)
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
		lifecycler1b, err := NewPartitionInstanceLifecycler(lifecycler1bConfig, "test", ringKey, store, logger, nil)
		require.NoError(t, err)
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
		}, 3*time.Second, eventuallyTick)
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
		lifecycler, err := NewPartitionInstanceLifecycler(lifecyclerConfig, "test", ringKey, store, logger, nil)
		require.NoError(t, err)
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
		lifecycler1a, err := NewPartitionInstanceLifecycler(lifecycler1aConfig, "test", ringKey, store, logger, nil)
		require.NoError(t, err)
		lifecycler1b, err := NewPartitionInstanceLifecycler(lifecycler1bConfig, "test", ringKey, store, logger, nil)
		require.NoError(t, err)
		lifecycler2a, err := NewPartitionInstanceLifecycler(lifecycler2aConfig, "test", ringKey, store, logger, nil)
		require.NoError(t, err)
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
		lifecycler, err := NewPartitionInstanceLifecycler(cfg, "test", ringKey, store, logger, nil)
		require.NoError(t, err)
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
		lifecycler, err := NewPartitionInstanceLifecycler(cfg, "test", ringKey, store, logger, nil)
		require.NoError(t, err)
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

	t.Run("should register and unregister with multi-owner ID when multiple partition ownership is enabled", func(t *testing.T) {
		t.Parallel()

		const instanceID1 = "instance-zone-a-1"
		const instanceID2 = "instance-zone-b-1"

		lifecyclerInstance1Partition1Config := createTestMultipartitionOwnershipPartitionInstanceLifecyclerConfig(1, instanceID1)
		lifecyclerInstance1Partition2Config := createTestMultipartitionOwnershipPartitionInstanceLifecyclerConfig(2, instanceID1)
		lifecyclerInstance2Partition1Config := createTestMultipartitionOwnershipPartitionInstanceLifecyclerConfig(1, instanceID2)

		store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })

		// Start instance-zone-a-1 lifecycler for partition 1.
		lifecyclerInstance1Partition1, err := NewPartitionInstanceLifecycler(lifecyclerInstance1Partition1Config, "test", ringKey, store, logger, nil)
		require.NoError(t, err)
		lifecyclerInstance1Partition1.SetRemoveOwnerOnShutdown(true)
		require.NoError(t, services.StartAndAwaitRunning(ctx, lifecyclerInstance1Partition1))

		assertMapElementsMatch(t, map[int32][]string{
			1: {multiPartitionOwnerInstanceID(instanceID1, 1)},
		}, getPartitionRingFromStore(t, store, ringKey).ownersByPartition())

		// Start instance-zone-a-1 lifecycler for partition 2.
		lifecyclerInstance1Partition2, err := NewPartitionInstanceLifecycler(lifecyclerInstance1Partition2Config, "test", ringKey, store, logger, nil)
		require.NoError(t, err)
		lifecyclerInstance1Partition2.SetRemoveOwnerOnShutdown(false)
		require.NoError(t, services.StartAndAwaitRunning(ctx, lifecyclerInstance1Partition2))

		assertMapElementsMatch(t, map[int32][]string{
			1: {multiPartitionOwnerInstanceID(instanceID1, 1)},
			2: {multiPartitionOwnerInstanceID(instanceID1, 2)},
		}, getPartitionRingFromStore(t, store, ringKey).ownersByPartition())

		// Start instance-zone-b-1 lifecycler for partition 1.
		lifecyclerInstance2Partition1, err := NewPartitionInstanceLifecycler(lifecyclerInstance2Partition1Config, "test", ringKey, store, logger, nil)
		require.NoError(t, err)
		lifecyclerInstance2Partition1.SetRemoveOwnerOnShutdown(true)
		require.NoError(t, services.StartAndAwaitRunning(ctx, lifecyclerInstance2Partition1))

		assertMapElementsMatch(t, map[int32][]string{
			1: {
				multiPartitionOwnerInstanceID(instanceID1, 1),
				multiPartitionOwnerInstanceID(instanceID2, 1),
			},
			2: {multiPartitionOwnerInstanceID(instanceID1, 2)},
		}, getPartitionRingFromStore(t, store, ringKey).ownersByPartition())

		// Shutdown lifecyclerInstance1Partition1, this should remove the owner for partition 1.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecyclerInstance1Partition1))

		assertMapElementsMatch(t, map[int32][]string{
			1: {multiPartitionOwnerInstanceID(instanceID2, 1)},
			2: {multiPartitionOwnerInstanceID(instanceID1, 2)},
		}, getPartitionRingFromStore(t, store, ringKey).ownersByPartition())

		// Shutdown lifecyclerInstance1Partition2, this should *NOT* remove the owner for partition 2, because "remove on shutdown" was disabled.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecyclerInstance1Partition2))
		assertMapElementsMatch(t, map[int32][]string{
			1: {multiPartitionOwnerInstanceID(instanceID2, 1)},
			2: {multiPartitionOwnerInstanceID(instanceID1, 2)},
		}, getPartitionRingFromStore(t, store, ringKey).ownersByPartition())

		// Shutdown lifecyclerInstance2Partition1, this should remove the owner for partition 1.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecyclerInstance2Partition1))
		assertMapElementsMatch(t, map[int32][]string{
			2: {multiPartitionOwnerInstanceID(instanceID1, 2)},
		}, getPartitionRingFromStore(t, store, ringKey).ownersByPartition())
	})
}

func assertMapElementsMatch[K cmp.Ordered, V any, S ~[]V](t *testing.T, expected map[K]S, actual map[K]S) {
	t.Helper()
	expectedKeys := slices.Sorted(maps.Keys(expected))
	actualKeys := slices.Sorted(maps.Keys(actual))
	require.Equal(t, expectedKeys, actualKeys, "expected keys do not match actual keys")

	for _, k := range expectedKeys {
		expectedValues := expected[k]
		actualValues := actual[k]
		assert.ElementsMatch(t, expectedValues, actualValues, "for key %v", k)
	}
}

func TestPartitionInstanceLifecycler_GetAndChangePartitionState(t *testing.T) {
	ctx := context.Background()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Start lifecycler.
	cfg := createTestPartitionInstanceLifecyclerConfig(1, "instance-1")
	lifecycler, err := NewPartitionInstanceLifecycler(cfg, "test", ringKey, store, log.NewNopLogger(), nil)
	require.NoError(t, err)
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

func TestPartitionInstanceLifecycler_DeleteInactivePartitionAfterDurationFunc(t *testing.T) {
	const eventuallyTick = 10 * time.Millisecond

	ctx := context.Background()

	for name, tc := range map[string]struct {
		staticDuration  time.Duration
		funcDuration    time.Duration
		funcErr         error
		expectedRemoved bool
	}{
		"func enables deletion when static duration is disabled": {
			staticDuration:  0,
			funcDuration:    time.Minute,
			expectedRemoved: true,
		},
		"func takes precedence over static duration that would delete the partition": {
			staticDuration:  time.Minute,
			funcDuration:    24 * time.Hour,
			expectedRemoved: false,
		},
		"func returning zero disables deletion despite static duration": {
			staticDuration:  time.Minute,
			funcDuration:    0,
			expectedRemoved: false,
		},
		"func error prevents deletion despite static duration": {
			staticDuration:  time.Minute,
			funcErr:         errors.New("failed to get duration"),
			expectedRemoved: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			// Seed the ring with an inactive partition with no owners, which has been inactive for an hour.
			require.NoError(t, store.CAS(ctx, ringKey, func(interface{}) (interface{}, bool, error) {
				desc := NewPartitionRingDesc()
				desc.AddPartition(2, PartitionInactive, time.Now().Add(-time.Hour))
				return desc, true, nil
			}))

			var calls atomic.Int64
			cfg := createTestPartitionInstanceLifecyclerConfig(1, "instance-1")
			cfg.DeleteInactivePartitionAfterDuration = tc.staticDuration
			cfg.DeleteInactivePartitionAfterDurationFunc = func() (time.Duration, error) {
				calls.Add(1)
				return tc.funcDuration, tc.funcErr
			}

			lifecycler, err := NewPartitionInstanceLifecycler(cfg, "test", ringKey, store, log.NewNopLogger(), nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
			t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler)) })

			if tc.expectedRemoved {
				assert.Eventually(t, func() bool {
					return !getPartitionRingFromStore(t, store, ringKey).HasPartition(2)
				}, 5*time.Second, eventuallyTick)
				return
			}

			// Wait until the func has been evaluated several times, then check the partition is still there.
			start := calls.Load()
			require.Eventually(t, func() bool { return calls.Load() >= start+3 }, 5*time.Second, eventuallyTick)
			assert.True(t, getPartitionRingFromStore(t, store, ringKey).HasPartition(2))
		})
	}
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

func createTestMultipartitionOwnershipPartitionInstanceLifecyclerConfig(partitionID int32, instanceID string) PartitionInstanceLifecyclerConfig {
	return PartitionInstanceLifecyclerConfig{
		PartitionID:                          partitionID,
		InstanceID:                           instanceID,
		MultiPartitionOwnership:              true,
		WaitOwnersCountOnPending:             0,
		WaitOwnersDurationOnPending:          0,
		DeleteInactivePartitionAfterDuration: 0,
		PollingInterval:                      10 * time.Millisecond,
	}
}

func TestNewPartitionInstanceLifecycler_ValidatesPartitionID(t *testing.T) {
	for _, tc := range []struct {
		partitionID   int32
		maxPartitions int32
		derived       bool
		valid         bool
	}{
		{partitionID: 0, maxPartitions: 8, derived: true, valid: true},
		{partitionID: 7, maxPartitions: 8, derived: true, valid: true},
		{partitionID: 8, maxPartitions: 8, derived: true, valid: false},
		{partitionID: 0, maxPartitions: 0, derived: true, valid: false},
		{partitionID: 0, maxPartitions: -1, derived: true, valid: false},
		{partitionID: 8, maxPartitions: 8, derived: false, valid: true},
		{partitionID: 0, maxPartitions: 0, derived: false, valid: true},
		{partitionID: -1, maxPartitions: 8, derived: true, valid: false},
		{partitionID: -1, maxPartitions: 8, derived: false, valid: false},
	} {
		t.Run(fmt.Sprintf("partition=%d/max=%d/derived=%t", tc.partitionID, tc.maxPartitions, tc.derived), func(t *testing.T) {
			cfg := derivedTokensLifecyclerConfig(tc.partitionID, tc.derived)
			cfg.MaxDerivedTokenPartitions = tc.maxPartitions
			lifecycler, err := NewPartitionInstanceLifecycler(cfg, "test", ringKey, nil, log.NewNopLogger(), nil)
			if tc.valid {
				require.NoError(t, err)
				assert.NotNil(t, lifecycler)
			} else {
				require.Error(t, err)
				assert.Nil(t, lifecycler)
			}
		})
	}
}

func TestPartitionInstanceLifecycler_CreatePartition(t *testing.T) {
	for _, tc := range []struct {
		derived        bool
		expectedScheme PartitionTokenScheme
		expectedTokens int
	}{
		{derived: false, expectedScheme: PartitionTokensStored, expectedTokens: optimalTokensPerInstance},
		{derived: true, expectedScheme: PartitionTokensSmt512, expectedTokens: 0},
	} {
		t.Run(fmt.Sprintf("derived=%t", tc.derived), func(t *testing.T) {
			desc := createPartitionAndRegisterOwnerForTest(t, nil, derivedTokensLifecyclerConfig(7, tc.derived))
			partition := desc.Partitions[7]
			assert.Equal(t, PartitionPending, partition.State)
			assert.Equal(t, tc.expectedScheme, partition.TokenScheme)
			assert.NotZero(t, partition.TokenSchemeTimestamp)
			assert.Len(t, partition.Tokens, tc.expectedTokens)
			assert.Equal(t, int32(7), desc.Owners["instance-1"].OwnedPartition)
		})
	}
}

func TestPartitionInstanceLifecycler_UpdateExistingPartitionTokenScheme(t *testing.T) {
	stored := PartitionDesc{Id: 1, State: PartitionActive, StateTimestamp: 1, Tokens: generateStoredPartitionTokens(1)}
	derived := PartitionDesc{Id: 1, State: PartitionActive, StateTimestamp: 1, TokenScheme: PartitionTokensSmt512}

	now := time.Now()
	storedLater := stored
	storedLater.TokenSchemeTimestamp = now.Add(30 * time.Second).Unix()
	storedTooFarInTheFuture := stored
	storedTooFarInTheFuture.TokenSchemeTimestamp = now.Add(maxFutureTimestampSkew + time.Minute).Unix()
	otherStoredTokens := PartitionDesc{Id: 1, State: PartitionActive, StateTimestamp: 1, Tokens: []uint32{1, 2, 3}}
	unknownScheme := PartitionDesc{Id: 1, State: PartitionActive, StateTimestamp: 1, TokenScheme: 99}

	for _, tc := range []struct {
		name            string
		existing        PartitionDesc
		useDerived      bool
		expected        PartitionDesc
		expectedUpdated bool
	}{
		{name: "stored tokens are kept", existing: stored, useDerived: false, expected: stored},
		{name: "derived tokens are kept", existing: derived, useDerived: true, expected: derived},
		{name: "stored tokens are converted to derived tokens", existing: stored, useDerived: true, expected: derived, expectedUpdated: true},
		{name: "derived tokens are converted to stored tokens", existing: derived, useDerived: false, expected: stored, expectedUpdated: true},
		{name: "stored tokens changed later are kept", existing: storedLater, useDerived: true, expected: storedLater},
		{name: "stored tokens changed too far in the future are converted to derived tokens", existing: storedTooFarInTheFuture, useDerived: true, expected: derived, expectedUpdated: true},
		{name: "stored tokens that differ from derived tokens are kept", existing: otherStoredTokens, useDerived: true, expected: otherStoredTokens},
		{name: "unknown token scheme is kept", existing: unknownScheme, useDerived: false, expected: unknownScheme},
	} {
		for _, createOnStartup := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/create on startup=%t", tc.name, createOnStartup), func(t *testing.T) {
				desc := startPartitionLifecyclerForTest(t, &tc.existing, derivedTokensLifecyclerConfig(1, tc.useDerived), createOnStartup)
				actual := desc.Partitions[1]
				if tc.expectedUpdated {
					assert.NotEqual(t, tc.existing.TokenSchemeTimestamp, actual.TokenSchemeTimestamp)
					actual.TokenSchemeTimestamp = tc.expected.TokenSchemeTimestamp
				}
				assert.Equal(t, tc.expected, actual)
				assert.Equal(t, int32(1), desc.Owners["instance-1"].OwnedPartition)
			})
		}
	}
}

func derivedTokensLifecyclerConfig(partitionID int32, derived bool) PartitionInstanceLifecyclerConfig {
	cfg := createTestPartitionInstanceLifecyclerConfig(partitionID, "instance-1")
	cfg.CreatePartitionsWithDerivedTokens = derived
	// Partitions 0-7 can be created with derived tokens.
	cfg.MaxDerivedTokenPartitions = 8
	return cfg
}

// createPartitionAndRegisterOwnerForTest runs the lifecycler against a ring that contains the existing partition,
// if not nil, and returns the resulting ring.
func createPartitionAndRegisterOwnerForTest(t *testing.T, existing *PartitionDesc, cfg PartitionInstanceLifecyclerConfig) *PartitionRingDesc {
	return startPartitionLifecyclerForTest(t, existing, cfg, true)
}

func startPartitionLifecyclerForTest(t *testing.T, existing *PartitionDesc, cfg PartitionInstanceLifecyclerConfig, createOnStartup bool) *PartitionRingDesc {
	t.Helper()
	ctx := context.Background()
	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	if existing != nil {
		require.NoError(t, store.CAS(ctx, ringKey, func(interface{}) (interface{}, bool, error) {
			desc := NewPartitionRingDesc()
			desc.Partitions[existing.Id] = *existing
			return desc, true, nil
		}))
	}
	return startPartitionLifecyclerWithStoreForTest(t, store, cfg, createOnStartup)
}

// startPartitionLifecyclerWithStoreForTest runs the lifecycler startup against store and returns the resulting ring.
func startPartitionLifecyclerWithStoreForTest(t *testing.T, store kv.Client, cfg PartitionInstanceLifecyclerConfig, createOnStartup bool) *PartitionRingDesc {
	t.Helper()
	lifecycler, err := NewPartitionInstanceLifecycler(cfg, "test", ringKey, store, log.NewNopLogger(), nil)
	require.NoError(t, err)
	lifecycler.SetCreatePartitionOnStartup(createOnStartup)
	require.NoError(t, lifecycler.starting(t.Context()))

	return getPartitionRingFromStore(t, store, ringKey)
}

// newPartitionRingMemberlistClientForTest returns a client of a single-node memberlist KV store, and the registry
// of the store's metrics. Unlike the in-memory Consul client, the store merges updates and keeps tombstones.
func newPartitionRingMemberlistClientForTest(t *testing.T) (kv.Client, *prometheus.Registry) {
	t.Helper()
	c := GetPartitionRingCodec()
	var cfg memberlist.KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = memberlist.TCPTransportConfig{BindAddrs: []string{"127.0.0.1"}}
	cfg.Codecs = []codec.Codec{c}
	// Keep tombstones for longer than any test runs, independently of the default.
	cfg.LeftIngestersTimeout = time.Hour
	reg := prometheus.NewRegistry()
	store := memberlist.NewKV(cfg, log.NewNopLogger(), nil, reg)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), store))
	// t.Context() is canceled before cleanup runs.
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(context.Background(), store)) })
	client, err := memberlist.NewClient(store, c)
	require.NoError(t, err)
	return client, reg
}

// A partition can be recreated while memberlist still holds the tombstone of its previous incarnation. Get hides
// tombstones, so the lifecycler creates the partition, and the merge into the tombstone must take the token scheme
// of the new partition.
func TestPartitionInstanceLifecycler_CreatePartition_ReplacesTombstoneTokenScheme(t *testing.T) {
	// In the past, so the new partition state isn't in the same second as the deletion, which would win.
	deletedAt := time.Now().Add(-time.Minute).Unix()
	storedTokens := generateStoredPartitionTokens(1)

	for _, tc := range []struct {
		name           string
		tombstone      PartitionDesc
		useDerived     bool
		expectedScheme PartitionTokenScheme
		expectedTokens []uint32
	}{
		{
			name:           "derived tombstone replaced by stored tokens",
			tombstone:      PartitionDesc{Id: 1, State: PartitionDeleted, StateTimestamp: deletedAt, TokenScheme: PartitionTokensSmt512, TokenSchemeTimestamp: deletedAt},
			useDerived:     false,
			expectedScheme: PartitionTokensStored,
			expectedTokens: storedTokens,
		},
		{
			name:           "stored tombstone replaced by derived tokens",
			tombstone:      PartitionDesc{Id: 1, State: PartitionDeleted, StateTimestamp: deletedAt, Tokens: storedTokens, TokenSchemeTimestamp: deletedAt},
			useDerived:     true,
			expectedScheme: PartitionTokensSmt512,
			expectedTokens: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, reg := newPartitionRingMemberlistClientForTest(t)
			require.NoError(t, store.CAS(t.Context(), ringKey, func(interface{}) (interface{}, bool, error) {
				desc := NewPartitionRingDesc()
				desc.Partitions[1] = tc.tombstone
				return desc, true, nil
			}))
			// Get hides tombstones, so check through the store's metrics that it holds the tombstone.
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
				# HELP memberlist_client_kv_store_value_tombstones Number of tombstones currently present in KV store values
				# TYPE memberlist_client_kv_store_value_tombstones gauge
				memberlist_client_kv_store_value_tombstones{key="`+ringKey+`"} 1
			`), "memberlist_client_kv_store_value_tombstones"))

			desc := startPartitionLifecyclerWithStoreForTest(t, store, derivedTokensLifecyclerConfig(1, tc.useDerived), true)
			actual := desc.Partitions[1]
			assert.Equal(t, PartitionPending, actual.State)
			assert.Equal(t, tc.expectedScheme, actual.TokenScheme)
			assert.Equal(t, tc.expectedTokens, actual.Tokens)
		})
	}
}
