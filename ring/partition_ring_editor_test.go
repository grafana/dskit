package ring

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/kv/consul"
)

func TestPartitionRingEditor_ChangePartitionState(t *testing.T) {
	ctx := context.Background()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Init the ring.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(1, PartitionActive, time.Now())
		desc.AddPartition(2, PartitionActive, time.Now())
		return desc, true, nil
	}))

	// Start editor.
	editor := NewPartitionRingEditor(ringKey, store)

	// Pre-condition: ensure the partitions are in the expected state at the beginning of the test.
	require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 1))
	require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 2))

	// A request to switch to state whose transition is not allowed should return error.
	require.ErrorIs(t, editor.ChangePartitionState(ctx, 1, PartitionPending), ErrPartitionStateChangeNotAllowed)
	require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 1))
	require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 2))

	// Switch to inactive.
	require.NoError(t, editor.ChangePartitionState(ctx, 1, PartitionInactive))
	require.Equal(t, PartitionInactive, getPartitionStateFromStore(t, store, ringKey, 1))
	require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 2))

	// A request to switch to the same state should be a no-op.
	require.NoError(t, editor.ChangePartitionState(ctx, 1, PartitionInactive))
	require.Equal(t, PartitionInactive, getPartitionStateFromStore(t, store, ringKey, 1))
	require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 2))
}

func TestPartitionRingEditor_RemoveMultiPartitionOwner(t *testing.T) {
	ctx := context.Background()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Init the ring with a partition and an owner.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(1, PartitionActive, time.Now())
		desc.AddOrUpdateOwner(multiPartitionOwnerInstanceID("instance-1", 1), OwnerActive, 1, time.Now())
		desc.AddOrUpdateOwner(multiPartitionOwnerInstanceID("instance-1", 2), OwnerActive, 2, time.Now())
		desc.AddOrUpdateOwner(multiPartitionOwnerInstanceID("instance-2", 1), OwnerActive, 1, time.Now())
		desc.AddOrUpdateOwner(multiPartitionOwnerInstanceID("instance-2", 2), OwnerActive, 2, time.Now())
		return desc, true, nil
	}))

	// This is what we should've built above.
	assertMapElementsMatch(t, map[int32][]string{
		1: {multiPartitionOwnerInstanceID("instance-1", 1), multiPartitionOwnerInstanceID("instance-2", 1)},
		2: {multiPartitionOwnerInstanceID("instance-1", 2), multiPartitionOwnerInstanceID("instance-2", 2)},
	}, getPartitionRingFromStore(t, store, ringKey).ownersByPartition())

	// Start editor.
	editor := NewPartitionRingEditor(ringKey, store)

	// Remove the instance-1 as an owner of partition 1.
	require.NoError(t, editor.RemoveMultiPartitionOwner(ctx, "instance-1", 1))

	// Verify the owner is removed.
	// This is what we should've built above.
	assertMapElementsMatch(t, map[int32][]string{
		1: {multiPartitionOwnerInstanceID("instance-2", 1)},
		2: {multiPartitionOwnerInstanceID("instance-1", 2), multiPartitionOwnerInstanceID("instance-2", 2)},
	}, getPartitionRingFromStore(t, store, ringKey).ownersByPartition())
}

func TestPartitionRingEditor_SetPartitionStateChangeLock(t *testing.T) {
	ctx := context.Background()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Init the ring.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(1, PartitionActive, time.Now())
		return desc, true, nil
	}))

	editor := NewPartitionRingEditor(ringKey, store)

	require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 1))

	// Lock the partition state change.
	require.NoError(t, editor.SetPartitionStateChangeLock(ctx, 1, true))

	// Try to change state, should fail.
	require.ErrorIs(t, editor.ChangePartitionState(ctx, 1, PartitionInactive), ErrPartitionStateChangeLocked)
	require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 1))

	// Unlock the partition state change.
	require.NoError(t, editor.SetPartitionStateChangeLock(ctx, 1, false))

	// Try to change state, should succeed.
	require.NoError(t, editor.ChangePartitionState(ctx, 1, PartitionInactive))
	require.Equal(t, PartitionInactive, getPartitionStateFromStore(t, store, ringKey, 1))
}
