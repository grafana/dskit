package ring

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionInstanceRing_GetReplicationSetsForOperation(t *testing.T) {
	now := time.Now()
	op := NewOp([]InstanceState{ACTIVE}, nil)
	heartbeatTimeout := time.Minute

	type comparableReplicationSet struct {
		instances           []string
		maxUnavailableZones int
	}

	tests := map[string]struct {
		partitionsRing PartitionRingDesc
		instancesRing  *Desc
		expectedErr    error
		expectedSets   []comparableReplicationSet
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
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
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
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					"instance-zone-a-1": {OwnedPartition: 1},
					"instance-zone-a-2": {OwnedPartition: 2},
					"instance-zone-b-2": {OwnedPartition: 2},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
				"instance-zone-a-2": {Id: "instance-zone-a-2", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
				"instance-zone-b-2": {Id: "instance-zone-b-2", State: ACTIVE, Zone: "b", Timestamp: now.Unix()},
			}},
			expectedSets: []comparableReplicationSet{
				{instances: []string{"instance-zone-a-1"}, maxUnavailableZones: 0},
				{instances: []string{"instance-zone-a-2", "instance-zone-b-2"}, maxUnavailableZones: 1},
			},
		},
		"should return error if there are no healthy instances for a partition": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					"instance-zone-a-1": {OwnedPartition: 1},
					"instance-zone-a-2": {OwnedPartition: 2},
					"instance-zone-b-2": {OwnedPartition: 2},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
				"instance-zone-a-2": {Id: "instance-zone-a-2", State: ACTIVE, Zone: "a", Timestamp: now.Add(-2 * time.Minute).Unix()}, // Unhealthy.
			}},
			expectedErr: ErrTooManyUnhealthyInstances,
		},
		"should return replication sets excluding unhealthy instances as long as there's at least 1 healthy instance per partition": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					"instance-zone-a-1": {OwnedPartition: 1},
					"instance-zone-b-1": {OwnedPartition: 1},
					"instance-zone-a-2": {OwnedPartition: 2},
					"instance-zone-b-2": {OwnedPartition: 2},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
				"instance-zone-b-1": {Id: "instance-zone-a-1", State: LEAVING, Zone: "a", Timestamp: now.Unix()}, // Unhealthy because of the state.
				"instance-zone-a-2": {Id: "instance-zone-a-2", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
				"instance-zone-b-2": {Id: "instance-zone-b-2", State: ACTIVE, Zone: "b", Timestamp: now.Add(-2 * time.Minute).Unix()}, // Unhealthy because of the heartbeat.
			}},
			expectedSets: []comparableReplicationSet{
				{instances: []string{"instance-zone-a-1"}, maxUnavailableZones: 0},
				{instances: []string{"instance-zone-a-2"}, maxUnavailableZones: 0},
			},
		},
		"should NOT return error if an instance is missing in the instances ring but there's another healthy instance for the partition": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					"instance-zone-a-1": {OwnedPartition: 1},
					"instance-zone-b-1": {OwnedPartition: 1}, // Missing in the instances ring.
					"instance-zone-a-2": {OwnedPartition: 2}, // Missing in the instances ring.
					"instance-zone-b-2": {OwnedPartition: 2},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "a", Timestamp: now.Unix()},
				"instance-zone-b-2": {Id: "instance-zone-b-2", State: ACTIVE, Zone: "b", Timestamp: now.Unix()},
			}},
			expectedSets: []comparableReplicationSet{
				{instances: []string{"instance-zone-a-1"}, maxUnavailableZones: 0},
				{instances: []string{"instance-zone-b-2"}, maxUnavailableZones: 0},
			},
		},
		"should return replication sets with MaxUnavailableZones=0 if there are multiple instances per zone but all instances belong to the same zone": {
			partitionsRing: PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {State: PartitionActive},
					2: {State: PartitionInactive},
				},
				Owners: map[string]OwnerDesc{
					"instance-zone-a-1": {OwnedPartition: 1},
					"instance-zone-b-1": {OwnedPartition: 1},
					"instance-zone-a-2": {OwnedPartition: 2},
					"instance-zone-b-2": {OwnedPartition: 2},
				},
			},
			instancesRing: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-zone-a-1": {Id: "instance-zone-a-1", State: ACTIVE, Zone: "fixed", Timestamp: now.Unix()},
				"instance-zone-b-1": {Id: "instance-zone-b-1", State: ACTIVE, Zone: "fixed", Timestamp: now.Unix()},
				"instance-zone-a-2": {Id: "instance-zone-a-2", State: ACTIVE, Zone: "fixed", Timestamp: now.Unix()},
				"instance-zone-b-2": {Id: "instance-zone-b-2", State: ACTIVE, Zone: "fixed", Timestamp: now.Unix()},
			}},
			expectedSets: []comparableReplicationSet{
				{instances: []string{"instance-zone-a-1", "instance-zone-b-1"}, maxUnavailableZones: 0},
				{instances: []string{"instance-zone-a-2", "instance-zone-b-2"}, maxUnavailableZones: 0},
			},
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
			actual := make([]comparableReplicationSet, 0, len(sets))
			for _, set := range sets {
				instanceIDs := set.GetIDs()
				slices.Sort(instanceIDs)
				actual = append(actual, comparableReplicationSet{instances: instanceIDs, maxUnavailableZones: set.MaxUnavailableZones})
			}

			assert.ElementsMatch(t, testData.expectedSets, actual)
		})
	}
}

func BenchmarkPartitionInstanceRing_GetReplicationSetsForOperation(b *testing.B) {
	var (
		numActivePartitions = 100
		zones               = []string{"a", "b", "c"}
		now                 = time.Now()
		readOnlyUpdated     = time.Time{}
	)

	// Create the instances and partitions ring.
	partitionsRing := NewPartitionRingDesc()
	instancesRing := &Ring{ringDesc: &Desc{}}

	for partitionID := 0; partitionID < numActivePartitions; partitionID++ {
		partitionsRing.AddPartition(int32(partitionID), PartitionActive, now)

		for _, zone := range zones {
			instanceID := fmt.Sprintf("instance-zone-%s-%d", zone, partitionID)
			instancesRing.ringDesc.AddIngester(instanceID, instanceID, zone, nil, ACTIVE, now, false, readOnlyUpdated, nil)
			partitionsRing.AddOrUpdateOwner(instanceID, OwnerActive, int32(partitionID), now)
		}
	}

	r := NewPartitionInstanceRing(newStaticPartitionRingReader(NewPartitionRing(*partitionsRing)), instancesRing, time.Hour)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		sets, err := r.GetReplicationSetsForOperation(Read)
		if err != nil {
			b.Fatal(err)
		}
		if len(sets) != numActivePartitions {
			b.Fatalf("expected %d replication sets but got %d", numActivePartitions, len(sets))
		}
	}
}

func TestPartitionInstanceRing_ShuffleShard(t *testing.T) {
	now := time.Now()

	partitionsRing := NewPartitionRingDesc()
	partitionsRing.AddPartition(1, PartitionActive, now.Add(-120*time.Minute))
	partitionsRing.AddPartition(2, PartitionActive, now.Add(-30*time.Minute))
	partitionsRing.AddPartition(3, PartitionActive, now.Add(-30*time.Minute))
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
