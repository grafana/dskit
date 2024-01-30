package ring

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/kv/memberlist"
)

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

			change, err := localCopy.merge(incomingCopy, testData.localCAS, now)
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

			change, err := localCopy.merge(incomingCopy, testData.localCAS, now)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedUpdatedLocal, localCopy)
			assert.Equal(t, testData.expectedChange, change)
		})
	}
}
