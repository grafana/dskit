package ring

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/kv/memberlist"
)

func TestPartitionRingDesc_tokens(t *testing.T) {
	desc := &PartitionRingDesc{
		Partitions: map[int32]PartitionDesc{
			1: {Tokens: []uint32{1, 5, 8}, State: PartitionActive, StateTimestamp: 10},
			2: {Tokens: []uint32{3, 4, 9}, State: PartitionActive, StateTimestamp: 20},
		},
		Owners: map[string]OwnerDesc{
			"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
			"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
		},
	}

	assert.Equal(t, Tokens{1, 3, 4, 5, 8, 9}, desc.tokens())
}

func TestPartitionRingDesc_partitionByToken(t *testing.T) {
	desc := &PartitionRingDesc{
		Partitions: map[int32]PartitionDesc{
			1: {Tokens: []uint32{1, 5, 8}, State: PartitionActive, StateTimestamp: 10},
			2: {Tokens: []uint32{3, 4, 9}, State: PartitionActive, StateTimestamp: 20},
		},
		Owners: map[string]OwnerDesc{
			"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
			"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
		},
	}

	assert.Equal(t, map[Token]int32{1: 1, 5: 1, 8: 1, 3: 2, 4: 2, 9: 2}, desc.partitionByToken())
}

func TestPartitionRingDesc_countPartitionsByState(t *testing.T) {
	t.Run("empty ring should return all states with 0 partitions each", func(t *testing.T) {
		desc := &PartitionRingDesc{}

		assert.Equal(t, map[PartitionState]int{PartitionActive: 0, PartitionInactive: 0}, desc.countPartitionsByState())
	})

	t.Run("ring with only active partitions should other states with 0 partitions each", func(t *testing.T) {
		desc := &PartitionRingDesc{
			Partitions: map[int32]PartitionDesc{
				1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
			},
			Owners: map[string]OwnerDesc{},
		}

		assert.Equal(t, map[PartitionState]int{PartitionActive: 1, PartitionInactive: 0}, desc.countPartitionsByState())
	})

	t.Run("ring with some partitions in each state should correctly report the count", func(t *testing.T) {
		desc := &PartitionRingDesc{
			Partitions: map[int32]PartitionDesc{
				1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				3: {Tokens: []uint32{7, 8, 9}, State: PartitionInactive, StateTimestamp: 30},
			},
			Owners: map[string]OwnerDesc{
				"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
				"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
			},
		}

		assert.Equal(t, map[PartitionState]int{PartitionActive: 2, PartitionInactive: 1}, desc.countPartitionsByState())
	})
}

func TestPartitionRingDesc_AddOrUpdateOwner(t *testing.T) {
	now := time.Now()

	t.Run("should add a new owner", func(t *testing.T) {
		desc := NewPartitionRingDesc()
		require.True(t, desc.AddOrUpdateOwner("instance-1", OwnerActive, 1, now))

		assert.Equal(t, &PartitionRingDesc{
			Partitions: map[int32]PartitionDesc{},
			Owners: map[string]OwnerDesc{
				"instance-1": {
					UpdatedTimestamp: now.Unix(),
					State:            OwnerActive,
					OwnedPartition:   1,
				},
			},
		}, desc)
	})

	t.Run("should update an existing owner", func(t *testing.T) {
		desc := NewPartitionRingDesc()
		require.True(t, desc.AddOrUpdateOwner("instance-1", OwnerActive, 1, now))

		// Update the owner.
		require.True(t, desc.AddOrUpdateOwner("instance-1", OwnerActive, 2, now.Add(time.Second)))

		assert.Equal(t, &PartitionRingDesc{
			Partitions: map[int32]PartitionDesc{},
			Owners: map[string]OwnerDesc{
				"instance-1": {
					UpdatedTimestamp: now.Add(time.Second).Unix(),
					State:            OwnerActive,
					OwnedPartition:   2,
				},
			},
		}, desc)
	})

	t.Run("should be a no-op if the owner already exist", func(t *testing.T) {
		desc := NewPartitionRingDesc()
		desc.AddOrUpdateOwner("instance-1", OwnerActive, 1, now)

		// Update the owner.
		require.False(t, desc.AddOrUpdateOwner("instance-1", OwnerActive, 1, now.Add(time.Second)))

		assert.Equal(t, &PartitionRingDesc{
			Partitions: map[int32]PartitionDesc{},
			Owners: map[string]OwnerDesc{
				"instance-1": {
					UpdatedTimestamp: now.Unix(), // Timestamp should not be updated.
					State:            OwnerActive,
					OwnedPartition:   1,
				},
			},
		}, desc)
	})
}

func TestPartitionRingDesc_Merge_AddPartition(t *testing.T) {
	tests := map[string]struct {
		local                *PartitionRingDesc
		incoming             *PartitionRingDesc
		expectedUpdatedLocal memberlist.Mergeable
		expectedChange       memberlist.Mergeable
	}{
		"the first partition is added without owners": {
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{},
				Owners:     map[string]OwnerDesc{},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{},
			},
		},
		"the first partition is added with owners": {
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{},
				Owners:     map[string]OwnerDesc{},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
				},
			},
		},
		"a new partition is added without owners": {
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{},
			},
		},
		"a new partition is added with owners": {
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, localCAS := range []bool{true, false} {
				t.Run(fmt.Sprintf("Local CAS: %t", localCAS), func(t *testing.T) {
					var (
						localCopy    = testData.local.Clone()
						incomingCopy = testData.incoming.Clone()
					)

					change, err := localCopy.Merge(incomingCopy, localCAS)
					require.NoError(t, err)
					assert.Equal(t, testData.expectedUpdatedLocal, localCopy)
					assert.Equal(t, testData.expectedChange, change)
				})
			}
		})
	}
}

func TestPartitionRingDesc_Merge_UpdatePartition(t *testing.T) {
	tests := map[string]struct {
		local                *PartitionRingDesc
		incoming             *PartitionRingDesc
		expectedUpdatedLocal memberlist.Mergeable
		expectedChange       memberlist.Mergeable
	}{
		"partition state changed with newer timestamp": {
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionInactive, StateTimestamp: 30}, // State changed with newer timestamp.
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionInactive, StateTimestamp: 30},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionInactive, StateTimestamp: 30},
				},
				Owners: map[string]OwnerDesc{},
			},
		},
		"partition state changed with older timestamp": {
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionInactive, StateTimestamp: 10}, // State changed with older timestamp.
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
			expectedChange: nil,
		},
		"partition state not changed but state timestamp updated with newer one": {
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 30}, // State timestamp updated with newer one.
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 30},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 15},
					"ingester-zone-b-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 25},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 30},
				},
				Owners: map[string]OwnerDesc{},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, localCAS := range []bool{true, false} {
				t.Run(fmt.Sprintf("Local CAS: %t", localCAS), func(t *testing.T) {
					var (
						localCopy    = testData.local.Clone()
						incomingCopy = testData.incoming.Clone()
					)

					change, err := localCopy.Merge(incomingCopy, localCAS)
					require.NoError(t, err)
					assert.Equal(t, testData.expectedUpdatedLocal, localCopy)
					assert.Equal(t, testData.expectedChange, change)
				})
			}
		})
	}
}

func TestPartitionRingDesc_Merge_RemovePartition(t *testing.T) {
	now := time.Unix(10000, 0)

	tests := map[string]struct {
		localCAS             bool
		local                *PartitionRingDesc
		incoming             *PartitionRingDesc
		expectedUpdatedLocal memberlist.Mergeable
		expectedChange       memberlist.Mergeable
	}{
		"local change: partition removed and local partition state is not deleted yet": {
			localCAS: true,
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					// Partition 2 removed.
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionDeleted, StateTimestamp: now.Unix()},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionDeleted, StateTimestamp: now.Unix()},
				},
				Owners: map[string]OwnerDesc{},
			},
		},
		"local change: partition removed and local partition state is already deleted": {
			localCAS: true,
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionDeleted, StateTimestamp: 20}, // Local state is already deleted.
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					// Partition 2 removed.
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionDeleted, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedChange: nil,
		},
		"incoming change: partition removed with newer timestamp": {
			localCAS: false,
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionDeleted, StateTimestamp: 30}, // Partition deleted with newer timestamp.
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionDeleted, StateTimestamp: 30},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionDeleted, StateTimestamp: 30},
				},
				Owners: map[string]OwnerDesc{},
			},
		},
		"incoming change: partition removed with equal timestamp, deletion should win": {
			localCAS: false,
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionDeleted, StateTimestamp: 20}, // Partition deleted with equal timestamp.
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionDeleted, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionDeleted, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{},
			},
		},
		"incoming change: partition removed with older timestamp": {
			localCAS: false,
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionDeleted, StateTimestamp: 10}, // Partition deleted with older timestamp.
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedChange: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var (
				localCopy    = testData.local.Clone().(*PartitionRingDesc)
				incomingCopy = testData.incoming.Clone()
			)

			change, err := localCopy.mergeWithTime(incomingCopy, testData.localCAS, now)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedUpdatedLocal, localCopy)
			assert.Equal(t, testData.expectedChange, change)
		})
	}
}

func TestPartitionRingDesc_Merge_AddOwner(t *testing.T) {
	tests := map[string]struct {
		local                *PartitionRingDesc
		incoming             *PartitionRingDesc
		expectedUpdatedLocal memberlist.Mergeable
		expectedChange       memberlist.Mergeable
	}{
		"add the first owner to a partition": {
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
				},
			},
		},
		"add the second owner to a partition": {
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{},
				Owners: map[string]OwnerDesc{
					"ingester-zone-b-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, localCAS := range []bool{true, false} {
				t.Run(fmt.Sprintf("Local CAS: %t", localCAS), func(t *testing.T) {
					var (
						localCopy    = testData.local.Clone()
						incomingCopy = testData.incoming.Clone()
					)

					change, err := localCopy.Merge(incomingCopy, localCAS)
					require.NoError(t, err)
					assert.Equal(t, testData.expectedUpdatedLocal, localCopy)
					assert.Equal(t, testData.expectedChange, change)
				})
			}
		})
	}
}

func TestPartitionRingDesc_Merge_RemoveOwner(t *testing.T) {
	now := time.Unix(10000, 0)

	tests := map[string]struct {
		localCAS             bool
		local                *PartitionRingDesc
		incoming             *PartitionRingDesc
		expectedUpdatedLocal memberlist.Mergeable
		expectedChange       memberlist.Mergeable
	}{
		"local change: owner removed and local owner state is not deleted yet": {
			localCAS: true,
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					// Owner ingester-zone-a-1 removed.
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: now.Unix()},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: now.Unix()},
				},
			},
		},
		"local change: partition removed and local partition state is already deleted": {
			localCAS: true,
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: 20}, // Local state is already deleted.
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					// Owner ingester-zone-a-1 removed.
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: 20},
				},
			},
			expectedChange: nil,
		},
		"incoming change: owner removed with newer timestamp": {
			localCAS: false,
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: 30}, // Owner deleted with newer timestamp.
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: 30},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: 30},
				},
			},
		},
		"incoming change: owner removed with equal timestamp, deletion should win": {
			localCAS: false,
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: 20}, // Owner deleted with equal timestamp.
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: 20},
				},
			},
			expectedChange: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: 20},
				},
			},
		},
		"incoming change: owner removed with older timestamp": {
			localCAS: false,
			local: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			incoming: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: 10}, // Owner deleted with older timestamp.
				},
			},
			expectedUpdatedLocal: &PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {Tokens: []uint32{1, 2, 3}, State: PartitionActive, StateTimestamp: 10},
					2: {Tokens: []uint32{4, 5, 6}, State: PartitionActive, StateTimestamp: 20},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
					"ingester-zone-a-1": {OwnedPartition: 2, State: OwnerActive, UpdatedTimestamp: 20},
				},
			},
			expectedChange: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var (
				localCopy    = testData.local.Clone().(*PartitionRingDesc)
				incomingCopy = testData.incoming.Clone()
			)

			change, err := localCopy.mergeWithTime(incomingCopy, testData.localCAS, now)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedUpdatedLocal, localCopy)
			assert.Equal(t, testData.expectedChange, change)
		})
	}
}

func TestPartitionRingDesc_Merge_EdgeCases(t *testing.T) {
	desc := NewPartitionRingDesc()

	actual, err := desc.Merge(nil, false)
	require.NoError(t, err)
	require.Nil(t, actual)

	actual, err = desc.Merge((*PartitionRingDesc)(nil), false)
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestPartitionRingDesc_RemoveTombstones(t *testing.T) {
	now := time.Now()

	createTestRing := func() *PartitionRingDesc {
		desc := NewPartitionRingDesc()
		desc.AddPartition(1, PartitionActive, now.Add(1*time.Second))
		desc.AddPartition(2, PartitionInactive, now.Add(2*time.Second))
		desc.AddPartition(3, PartitionDeleted, now.Add(3*time.Second))
		desc.AddOrUpdateOwner("owner-1", OwnerActive, 1, now.Add(4*time.Second))
		desc.AddOrUpdateOwner("owner-2", OwnerActive, 2, now.Add(4*time.Second))
		desc.AddOrUpdateOwner("owner-3", OwnerDeleted, 3, now.Add(4*time.Second))
		return desc
	}

	t.Run("should remove all tombstones when limit is zero value", func(t *testing.T) {
		desc := createTestRing()
		total, removed := desc.RemoveTombstones(time.Time{})
		assert.Equal(t, 0, total)
		assert.Equal(t, 2, removed)
		assert.False(t, desc.HasPartition(3))
		assert.False(t, desc.HasOwner("owner-3"))
	})

	t.Run("should remove tombstones older or equal to the limit when specified", func(t *testing.T) {
		desc := createTestRing()

		total, removed := desc.RemoveTombstones(now)
		assert.Equal(t, 2, total)
		assert.Equal(t, 0, removed)
		assert.True(t, desc.HasPartition(3))
		assert.True(t, desc.HasOwner("owner-3"))

		total, removed = desc.RemoveTombstones(now.Add(3 * time.Second))
		assert.Equal(t, 1, total)
		assert.Equal(t, 1, removed)
		assert.False(t, desc.HasPartition(3))
		assert.True(t, desc.HasOwner("owner-3"))

		total, removed = desc.RemoveTombstones(now.Add(4 * time.Second))
		assert.Equal(t, 0, total)
		assert.Equal(t, 1, removed)
		assert.False(t, desc.HasPartition(3))
		assert.False(t, desc.HasOwner("owner-3"))
	})
}
