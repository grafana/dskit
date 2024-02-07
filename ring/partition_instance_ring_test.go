package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestPartitionInstanceRing_GetReplicationSetsForOperation(t *testing.T) {
	now := time.Now()
	op := NewOp([]InstanceState{ACTIVE}, nil)
	heartbeatTimeout := time.Minute

	tests := map[string]struct {
		partitionsRing PartitionRingDesc
		instancesRing  *Desc
		expectedErr    error
		expectedSets   [][]string
	}{
		"should return error on empty partitions ring": {
			partitionsRing: PartitionRingDesc{},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-1": {Id: "instance-1", State: ACTIVE, Timestamp: now.Unix()},
				"instance-2": {Id: "instance-2", State: ACTIVE, Timestamp: now.Unix()},
			}},
			expectedErr: ErrEmptyRing,
		},
		"should return error on empty instances ring": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionReadWrite},
					2: {State: PartitionReadOnly},
				},
				Owners: map[string]OwnerDesc{
					"instance-1": {OwnedPartition: 1},
					"instance-2": {OwnedPartition: 2},
				},
			},
			instancesRing: &Desc{},
			expectedErr:   ErrTooManyUnhealthyInstances,
		},
		"should return replication sets with at least 1 instance per partition, if every partition has at least 1 healthy instance": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionReadWrite},
					2: {State: PartitionReadOnly},
				},
				Owners: map[string]OwnerDesc{
					"instance-1-a": {OwnedPartition: 1},
					"instance-2-a": {OwnedPartition: 2},
					"instance-2-b": {OwnedPartition: 2},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-1-a": {Id: "instance-1-a", State: ACTIVE, Timestamp: now.Unix()},
				"instance-2-a": {Id: "instance-2-a", State: ACTIVE, Timestamp: now.Unix()},
				"instance-2-b": {Id: "instance-2-b", State: ACTIVE, Timestamp: now.Unix()},
			}},
			expectedSets: [][]string{{"instance-1-a"}, {"instance-2-a", "instance-2-b"}},
		},
		"should return error if there are no healthy instances for a partition": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionReadWrite},
					2: {State: PartitionReadOnly},
				},
				Owners: map[string]OwnerDesc{
					"instance-1-a": {OwnedPartition: 1},
					"instance-2-a": {OwnedPartition: 2},
					"instance-2-b": {OwnedPartition: 2},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-1-a": {Id: "instance-1-a", State: ACTIVE, Timestamp: now.Unix()},
				"instance-2-a": {Id: "instance-2-a", State: ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix()}, // Unhealthy.
			}},
			expectedErr: ErrTooManyUnhealthyInstances,
		},
		"should return replication sets excluding unhealthy instances as long as there's at least 1 healthy instance per partition": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionReadWrite},
					2: {State: PartitionReadOnly},
				},
				Owners: map[string]OwnerDesc{
					"instance-1-a": {OwnedPartition: 1},
					"instance-1-b": {OwnedPartition: 1},
					"instance-2-a": {OwnedPartition: 2},
					"instance-2-b": {OwnedPartition: 2},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-1-a": {Id: "instance-1-a", State: ACTIVE, Timestamp: now.Unix()},
				"instance-1-b": {Id: "instance-1-a", State: LEAVING, Timestamp: now.Unix()}, // Unhealthy because of the state.
				"instance-2-a": {Id: "instance-2-a", State: ACTIVE, Timestamp: now.Unix()},
				"instance-2-b": {Id: "instance-2-b", State: ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix()}, // Unhealthy because of the heartbeat.
			}},
			expectedSets: [][]string{{"instance-1-a"}, {"instance-2-a"}},
		},
		"should NOT return error if an instance is missing in the instances ring but there's another healthy instance for the partition": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionReadWrite},
					2: {State: PartitionReadOnly},
				},
				Owners: map[string]OwnerDesc{
					"instance-1-a": {OwnedPartition: 1},
					"instance-1-b": {OwnedPartition: 1}, // Missing in the instances ring.
					"instance-2-a": {OwnedPartition: 2}, // Missing in the instances ring.
					"instance-2-b": {OwnedPartition: 2},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-1-a": {Id: "instance-1-a", State: ACTIVE, Timestamp: now.Unix()},
				"instance-2-b": {Id: "instance-2-b", State: ACTIVE, Timestamp: now.Unix()},
			}},
			expectedSets: [][]string{{"instance-1-a"}, {"instance-2-b"}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			partitionsRing := NewPartitionRing(testData.partitionsRing)
			instancesRing := &Ring{ringDesc: testData.instancesRing}
			r := NewPartitionInstanceRing(newStaticPartitionRingReader(partitionsRing), instancesRing, heartbeatTimeout)

			sets, err := r.GetReplicationSetsForOperation(op)
			require.ErrorIs(t, err, testData.expectedErr)

			// Build the actual replication sets to compare with the expected ones.
			actual := make([][]string, 0, len(sets))
			for _, set := range sets {
				instanceIDs := set.GetIDs()
				slices.Sort(instanceIDs)
				actual = append(actual, instanceIDs)
			}

			assert.ElementsMatch(t, testData.expectedSets, actual)
		})
	}
}

func TestPartitionInstanceRing_ShuffleShard(t *testing.T) {
	now := time.Now()

	partitionsRing := NewPartitionRingDesc()
	partitionsRing.AddPartition(1, PartitionReadWrite, now.Add(-120*time.Minute))
	partitionsRing.AddPartition(2, PartitionReadWrite, now.Add(-30*time.Minute))
	partitionsRing.AddPartition(3, PartitionReadWrite, now.Add(-30*time.Minute))
	partitionsRing.AddOrUpdateOwner("instance-1", OwnerActive, 1, now.Add(-30*time.Minute))
	partitionsRing.AddOrUpdateOwner("instance-2", OwnerActive, 2, now.Add(-30*time.Minute))
	partitionsRing.AddOrUpdateOwner("instance-3", OwnerActive, 3, now.Add(-30*time.Minute))

	instancesRing := &Desc{Ingesters: map[string]InstanceDesc{
		"instance-1": {Id: "instance-1", State: ACTIVE, Timestamp: time.Now().Unix()},
		"instance-2": {Id: "instance-2", State: ACTIVE, Timestamp: time.Now().Unix()},
		"instance-3": {Id: "instance-3", State: ACTIVE, Timestamp: time.Now().Unix()},
	}}

	r := NewPartitionInstanceRing(newStaticPartitionRingReader(NewPartitionRing(*partitionsRing)), &Ring{ringDesc: instancesRing}, 0)

	t.Run("ShuffleShard()", func(t *testing.T) {
		actual, err := r.ShuffleShard("test", 2)
		require.NoError(t, err)
		assert.Equal(t, 2, actual.PartitionRing().PartitionsCount())
		assert.Equal(t, 3, actual.InstanceRing().InstancesCount()) // Should be preserved.
	})

	t.Run("ShuffleShardWithLookback()", func(t *testing.T) {
		actual, err := r.ShuffleShardWithLookback("test", 2, time.Hour, now)
		require.NoError(t, err)
		assert.Equal(t, 3, actual.PartitionRing().PartitionsCount())
		assert.Equal(t, 3, actual.InstanceRing().InstancesCount()) // Should be preserved.
	})
}
