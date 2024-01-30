package ring

import (
	"fmt"
	"time"
)

type PartitionInstanceRing struct {
	partitionsRing   *PartitionRingWatcher
	instancesRing    *Ring
	heartbeatTimeout time.Duration
}

func NewPartitionInstanceRing(partitionsRing *PartitionRingWatcher, instancesRing *Ring, heartbeatTimeout time.Duration) *PartitionInstanceRing {
	return &PartitionInstanceRing{
		partitionsRing:   partitionsRing,
		instancesRing:    instancesRing,
		heartbeatTimeout: heartbeatTimeout,
	}
}

// GetReplicationSetsForOperation returns one ReplicationSet for each partition and returns ReplicationSet for all partitions.
// If there are not enough owners for partitions, error is returned.
//
// For querying instances, basic idea is that we need to query *ALL* partitions in the ring (or subring).
// For each partition, each owner is a full replica, so it's enough to query single instance only.
// GetReplicationSetsForOperation returns all healthy owners for each partition according to op and the heartbeat timeout.
// GetReplicationSetsForOperation returns an error which Is(ErrTooManyUnhealthyInstances) if there are no healthy owners for some partition.
// GetReplicationSetsForOperation returns ErrEmptyRing if there are no partitions in the ring.
func (pr *PartitionInstanceRing) GetReplicationSetsForOperation(op Operation) ([]ReplicationSet, error) {
	// TODO cleanup this design to avoid having to access to .desc
	partitionsRing := pr.partitionsRing.GetRing()
	partitionsRingDesc := partitionsRing.desc
	partitionsRingOwners := partitionsRingDesc.PartitionOwners()

	if len(partitionsRingDesc.Partitions) == 0 {
		return nil, ErrEmptyRing
	}
	now := time.Now()

	result := make([]ReplicationSet, 0, len(partitionsRingDesc.Partitions))
	for pid := range partitionsRingDesc.Partitions {
		owners := partitionsRingOwners[pid]
		instances := make([]InstanceDesc, 0, len(owners))

		for _, instanceID := range owners {
			instance, err := pr.instancesRing.GetInstance(instanceID)
			if err != nil {
				return nil, err
			}

			if !instance.IsHealthy(op, pr.heartbeatTimeout, now) {
				continue
			}

			instances = append(instances, instance)
		}

		if len(instances) == 0 {
			return nil, fmt.Errorf("partition %d: %w", pid, ErrTooManyUnhealthyInstances)
		}

		result = append(result, ReplicationSet{
			Instances:            instances,
			MaxUnavailableZones:  len(instances) - 1, // We need response from at least 1 owner.
			ZoneAwarenessEnabled: true,
		})
	}
	return result, nil
}
