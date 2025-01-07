package v2

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
