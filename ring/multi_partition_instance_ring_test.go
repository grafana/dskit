package ring

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiPartitionInstanceRing_GetReplicationSetForPartitionAndOperation_HighestPreferrablyNonReadOnlyFromEachZone(t *testing.T) {
	now := time.Now()
	op := NewOp([]InstanceState{ACTIVE}, nil)
	heartbeatTimeout := time.Minute

	type comparableReplicationSet struct {
		instances           []string
		maxUnavailableZones int
	}

	const testPartitionID = 1

	tests := map[string]struct {
		partitionsRing PartitionRingDesc
		instancesRing  *Desc
		expectedErr    error
		expectedSet    comparableReplicationSet
	}{
		"should return error on empty partitions ring": {
			partitionsRing: PartitionRingDesc{},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
				"instance-zone-a-2": {Id: "instance-zone-a-2", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
			}},
			expectedErr: ErrEmptyRing,
		},
		"should return error on empty instances ring": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					multiPartitionOwnerInstanceID("instance-zone-a-1", 1): {OwnedPartition: 1},
					multiPartitionOwnerInstanceID("instance-zone-a-1", 2): {OwnedPartition: 2},
				},
			},
			instancesRing: &Desc{},
			expectedErr:   ErrTooManyUnhealthyInstances,
		},
		"should return replication sets with at least 1 instance for the partition, if the partition has at least 1 healthy instance": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					multiPartitionOwnerInstanceID("instance-zone-a-1", 1): {OwnedPartition: 1},
					multiPartitionOwnerInstanceID("instance-zone-a-1", 2): {OwnedPartition: 2},
					multiPartitionOwnerInstanceID("instance-zone-b-1", 1): {OwnedPartition: 1},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
				"instance-zone-b-1": {Id: "instance-zone-b-1", State: ACTIVE, Zone: "b", Timestamp: now.Unix()},
			}},
			expectedSet: comparableReplicationSet{instances: []string{"instance-zone-a-1", "instance-zone-b-1"}, maxUnavailableZones: 1},
		},
		"should return error if there are no healthy instances for the partition": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					multiPartitionOwnerInstanceID("instance-zone-a-1", 1): {OwnedPartition: 1},
					multiPartitionOwnerInstanceID("instance-zone-a-1", 2): {OwnedPartition: 2},
					multiPartitionOwnerInstanceID("instance-zone-b-1", 1): {OwnedPartition: 1},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "a", Timestamp: now.Add(-2 * time.Minute).Unix()}, // Unhealthy.
				"instance-zone-b-1": {Id: "instance-zone-b-1", State: ACTIVE, Zone: "b", Timestamp: now.Add(-3 * time.Minute).Unix()}, // Unhealthy.
			}},
			expectedErr: ErrTooManyUnhealthyInstances,
		},
		"should return the replication set excluding unhealthy instances as long as there's at least 1 healthy instance for the partition": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					multiPartitionOwnerInstanceID("instance-zone-a-1", 1): {OwnedPartition: 1},
					multiPartitionOwnerInstanceID("instance-zone-a-1", 2): {OwnedPartition: 2},
					multiPartitionOwnerInstanceID("instance-zone-b-1", 1): {OwnedPartition: 1},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "a", Timestamp: now.Add(-2 * time.Minute).Unix()}, // Unhealthy.
				"instance-zone-b-1": {Id: "instance-zone-b-1", State: ACTIVE, Zone: "b", Timestamp: now.Unix()},                       // Healthy.
			}},
			expectedSet: comparableReplicationSet{instances: []string{"instance-zone-b-1"}, maxUnavailableZones: 0},
		},
		"should return the highest owner from each zone for the partition": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					multiPartitionOwnerInstanceID("instance-zone-a-1", 1): {OwnedPartition: 1},
					multiPartitionOwnerInstanceID("instance-zone-a-2", 1): {OwnedPartition: 1}, // Highest owner in zone 'a'.
					multiPartitionOwnerInstanceID("instance-zone-b-1", 1): {OwnedPartition: 1},
					multiPartitionOwnerInstanceID("instance-zone-b-2", 2): {OwnedPartition: 2}, // Different partition.
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
				"instance-zone-a-2": {Id: "instance-zone-a-2", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
				"instance-zone-b-1": {Id: "instance-zone-b-1", State: ACTIVE, Zone: "b", Timestamp: now.Unix()},
				"instance-zone-b-2": {Id: "instance-zone-b-2", State: ACTIVE, Zone: "b", Timestamp: now.Unix()},
			}},
			expectedSet: comparableReplicationSet{instances: []string{"instance-zone-a-2", "instance-zone-b-1"}, maxUnavailableZones: 1},
		},
		"should return the highest non read-only owner from each zone for the partition": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					multiPartitionOwnerInstanceID("instance-zone-a-1", 1): {OwnedPartition: 1}, // Highest non-read-only owner in zone 'a'.
					multiPartitionOwnerInstanceID("instance-zone-a-2", 1): {OwnedPartition: 1}, // Highest owner in zone 'a' but it's read-only.
					multiPartitionOwnerInstanceID("instance-zone-b-1", 1): {OwnedPartition: 1},
					multiPartitionOwnerInstanceID("instance-zone-b-2", 2): {OwnedPartition: 2}, // Different partition.
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
				"instance-zone-a-2": {Id: "instance-zone-a-2", State: ACTIVE, Zone: "a", Timestamp: now.Unix(), ReadOnly: true}, // Read-only.
				"instance-zone-b-1": {Id: "instance-zone-b-1", State: ACTIVE, Zone: "b", Timestamp: now.Unix()},
				"instance-zone-b-2": {Id: "instance-zone-b-2", State: ACTIVE, Zone: "b", Timestamp: now.Unix()},
			}},
			expectedSet: comparableReplicationSet{instances: []string{"instance-zone-a-1", "instance-zone-b-1"}, maxUnavailableZones: 1},
		},
		"should return the read-only owner if it's the only one": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					multiPartitionOwnerInstanceID("instance-zone-a-2", 1): {OwnedPartition: 1}, // Read-only but it's the only owner in zone 'a'.
					multiPartitionOwnerInstanceID("instance-zone-b-1", 1): {OwnedPartition: 1},
					multiPartitionOwnerInstanceID("instance-zone-b-2", 2): {OwnedPartition: 2}, // Different partition.
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},                 // Does not own the partition.
				"instance-zone-a-2": {Id: "instance-zone-a-2", State: ACTIVE, Zone: "a", Timestamp: now.Unix(), ReadOnly: true}, // Read-only.
				"instance-zone-b-1": {Id: "instance-zone-b-1", State: ACTIVE, Zone: "b", Timestamp: now.Unix()},
				"instance-zone-b-2": {Id: "instance-zone-b-2", State: ACTIVE, Zone: "b", Timestamp: now.Unix()},
			}},
			expectedSet: comparableReplicationSet{instances: []string{"instance-zone-a-2", "instance-zone-b-1"}, maxUnavailableZones: 1},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			partitionsRing := NewPartitionRing(testData.partitionsRing)
			instancesRing := &Ring{ringDesc: testData.instancesRing}
			r := NewMultiPartitionInstanceRing(newStaticPartitionRingReader(partitionsRing), instancesRing, heartbeatTimeout)

			set, err := r.GetReplicationSetForPartitionAndOperation(testPartitionID, op)
			require.ErrorIs(t, err, testData.expectedErr)
			if testData.expectedErr != nil {
				return
			}

			// Build the actual replication sets to compare with the expected ones.
			instanceIDs := set.GetIDs()
			slices.Sort(instanceIDs)
			actual := comparableReplicationSet{instances: instanceIDs, maxUnavailableZones: set.MaxUnavailableZones}

			assert.Equal(t, testData.expectedSet, actual)
		})
	}
}
