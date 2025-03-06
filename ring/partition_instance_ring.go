package ring

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"
)

type PartitionRingReader interface {
	// PartitionRing returns a snapshot of the PartitionRing. This function must never return nil.
	// If the ring is empty or unknown, an empty PartitionRing can be returned.
	PartitionRing() *PartitionRing
}

type InstanceRingReader interface {
	// GetInstance return the InstanceDesc for the given instanceID or an error
	// if the instance doesn't exist in the ring. The returned InstanceDesc is NOT a
	// deep copy, so the caller should never modify it.
	GetInstance(string) (InstanceDesc, error)

	// InstancesCount returns the number of instances in the ring.
	InstancesCount() int
}

// PartitionInstanceRing holds a partitions ring and a instances ring, and provide functions
// to look up the intersection of the two (e.g. healthy instances by partition).
type PartitionInstanceRing struct {
	partitionsRingReader PartitionRingReader
	instancesRing        InstanceRingReader
	heartbeatTimeout     time.Duration

	// multiplePartitions is true if instances may own multiple partitions.
	// In this case, instance IDs should have a suffix starting with a slash which will be removed before looking up
	// the instance in the instancesRing.
	multiplePartitions bool
}

func NewPartitionInstanceRing(partitionsRingWatcher PartitionRingReader, instancesRing InstanceRingReader, heartbeatTimeout time.Duration) *PartitionInstanceRing {
	return &PartitionInstanceRing{
		partitionsRingReader: partitionsRingWatcher,
		instancesRing:        instancesRing,
		heartbeatTimeout:     heartbeatTimeout,
	}
}

func NewMultiPartitionInstanceRing(partitionsRingWatcher PartitionRingReader, instancesRing InstanceRingReader, heartbeatTimeout time.Duration) *PartitionInstanceRing {
	return &PartitionInstanceRing{
		partitionsRingReader: partitionsRingWatcher,
		instancesRing:        instancesRing,
		heartbeatTimeout:     heartbeatTimeout,
		multiplePartitions:   true,
	}
}

func (r *PartitionInstanceRing) PartitionRing() *PartitionRing {
	return r.partitionsRingReader.PartitionRing()
}

func (r *PartitionInstanceRing) InstanceRing() InstanceRingReader {
	return r.instancesRing
}

// GetReplicationSetsForOperation returns one ReplicationSet for each partition in the ring.
// A ReplicationSet is returned for every partition in ring. If there are no healthy owners
// for a partition, an error is returned.
func (r *PartitionInstanceRing) GetReplicationSetsForOperation(op Operation) ([]ReplicationSet, error) {
	partitionsRing := r.PartitionRing()
	partitionsRingDesc := partitionsRing.desc

	if len(partitionsRingDesc.Partitions) == 0 {
		return nil, ErrEmptyRing
	}

	now := time.Now()
	result := make([]ReplicationSet, 0, len(partitionsRingDesc.Partitions))
	zonesBuffer := make([]string, 0, 3) // Pre-allocate buffer assuming 3 zones.

	for partitionID := range partitionsRingDesc.Partitions {
		replicationSet, err := r.getReplicationSetForPartitionAndOperation(partitionID, op, false, now, zonesBuffer)
		if err != nil {
			return nil, err
		}

		result = append(result, replicationSet)
	}
	return result, nil
}

// GetReplicationSetForPartitionAndOperation returns a ReplicationSet for the input partition. If the partition doesn't
// exist or there are no healthy owners for the partition, an error is returned.
// pickHighestPreferrablyNonReadOnlyOwnerFromEachZone instructs to pick the highest instance from each zone when building the replication set,
// non-read-only instances are preferred over read-only instances (this is more important than picking the highest instance).
func (r *PartitionInstanceRing) GetReplicationSetForPartitionAndOperation(partitionID int32, op Operation, pickHighestPreferrablyNonReadOnlyOwnerFromEachZone bool) (ReplicationSet, error) {
	var stackZonesBuffer [3]string // Pre-allocate buffer assuming 3 zones.

	return r.getReplicationSetForPartitionAndOperation(partitionID, op, pickHighestPreferrablyNonReadOnlyOwnerFromEachZone, time.Now(), stackZonesBuffer[:])
}

func (r *PartitionInstanceRing) getReplicationSetForPartitionAndOperation(partitionID int32, op Operation, pickHighestPreferrablyNonReadOnlyOwnerFromEachZone bool, now time.Time, zonesBuffer []string) (ReplicationSet, error) {
	const maxExpectedPartitionOwners = 16
	var ownerIDs []string
	var stackOwnerIDs [maxExpectedPartitionOwners]string
	if r.multiplePartitions {
		ownerIDs = r.PartitionRing().MultiPartitionOwnerIDs(partitionID, stackOwnerIDs[:0])
	} else {
		nonModifyableOwnerIDs := r.PartitionRing().PartitionOwnerIDs(partitionID)
		ownerIDs = append(stackOwnerIDs[:0], nonModifyableOwnerIDs...)
	}
	instances := make([]InstanceDesc, 0, len(ownerIDs))

	for _, instanceID := range ownerIDs {
		instance, err := r.instancesRing.GetInstance(instanceID)
		if err != nil {
			// If an instance doesn't exist in the instances ring we don't return an error
			// but lookup for other instances of the partition.
			continue
		}

		if !instance.IsHealthy(op, r.heartbeatTimeout, now) {
			continue
		}

		instances = append(instances, instance)
		// Store this in the same position as the instance, so we can use it to compare later.
		// We only need this when r.pickHighestZoneOwner=true, but it's probably cheaper to do it than to check it.
		ownerIDs[len(instances)-1] = instanceID
	}

	if len(instances) == 0 {
		return ReplicationSet{}, fmt.Errorf("partition %d: %w", partitionID, ErrTooManyUnhealthyInstances)
	}

	// Count the number of unique zones among instances.
	zonesBuffer = uniqueZonesFromInstances(instances, zonesBuffer[:0])
	uniqueZones := len(zonesBuffer)

	if pickHighestPreferrablyNonReadOnlyOwnerFromEachZone {
		instances = highestPreferrablyNonReadOnlyFromEachZone(instances, ownerIDs, zonesBuffer)
	}

	return ReplicationSet{
		Instances: instances,

		// Partitions has no concept of zone, but we enable it in order to support ring's requests
		// minimization feature.
		ZoneAwarenessEnabled: true,

		// We need response from at least 1 owner. The assumption is that we have 1 owner per zone
		// but it's not guaranteed (depends on how the application was deployed). The safest thing
		// we can do here is to just request a successful response from at least 1 zone.
		MaxUnavailableZones: uniqueZones - 1,
	}, nil
}

// this method expects instanceIDs to be in the same order as instances.
// instanceIDs should hold the parsed multi-partition owner IDs.
func highestPreferrablyNonReadOnlyFromEachZone(instances []InstanceDesc, instanceIDs []string, instanceZones []string) []InstanceDesc {
	var stackAllInstances [16]InstanceDesc
	allInstances := append(stackAllInstances[:0], instances...)
	instances = instances[:0] // Reset, this is what we're going to return.
	for _, zone := range instanceZones {
		highest := -1
		for i, instance := range allInstances {
			if instance.Zone != zone {
				// Not our zone.
				continue
			}

			// Always pick the first one.
			if highest == -1 {
				highest = i
				continue
			}

			// If highest is read-only, and this one is not, pick it without checking the order.
			if allInstances[highest].ReadOnly && !instance.ReadOnly {
				highest = i
				continue
			}

			// If this one is read only, and highest is not, skip it.
			if instance.ReadOnly && !allInstances[highest].ReadOnly {
				continue
			}

			// If this one has a lower index than the one we have, skip it.
			if indexFromInstanceSuffix(instanceIDs[i]) < indexFromInstanceSuffix(instanceIDs[highest]) {
				continue
			}

			// This one is better than the one we have.
			highest = i
		}

		// We know there's always a highest instance for a zone, because zones were calculated from instances.
		instances = append(instances, allInstances[highest])
	}
	return instances
}

// indexFromInstanceSuffix returns the index from the instance suffix. The suffix is expected to be a number.
func indexFromInstanceSuffix(instance string) int {
	digitsSuffixLen := 0
	for digitsSuffixLen < len(instance) {
		i := len(instance) - digitsSuffixLen - 1
		if instance[i] >= '0' && instance[i] <= '9' {
			digitsSuffixLen++
		} else {
			break
		}
	}
	if digitsSuffixLen == 0 {
		return math.MaxInt64
	}
	index, err := strconv.Atoi(instance[len(instance)-digitsSuffixLen:])
	if err != nil {
		return math.MaxInt64 // Shouldn't happen, we already made sure that the suffix is a number.
	}
	return index
}

// ShuffleShard wraps PartitionRing.ShuffleShard().
//
// The PartitionRing embedded in the returned PartitionInstanceRing is based on a snapshot of the partitions ring
// at the time this function gets called. This means that subsequent changes to the partitions ring will not
// be reflected in the returned PartitionInstanceRing.
func (r *PartitionInstanceRing) ShuffleShard(identifier string, size int) (*PartitionInstanceRing, error) {
	partitionsSubring, err := r.PartitionRing().ShuffleShard(identifier, size)
	if err != nil {
		return nil, err
	}

	return NewPartitionInstanceRing(newStaticPartitionRingReader(partitionsSubring), r.instancesRing, r.heartbeatTimeout), nil
}

// ShuffleShardWithLookback wraps PartitionRing.ShuffleShardWithLookback().
//
// The PartitionRing embedded in the returned PartitionInstanceRing is based on a snapshot of the partitions ring
// at the time this function gets called. This means that subsequent changes to the partitions ring will not
// be reflected in the returned PartitionInstanceRing.
func (r *PartitionInstanceRing) ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) (*PartitionInstanceRing, error) {
	partitionsSubring, err := r.PartitionRing().ShuffleShardWithLookback(identifier, size, lookbackPeriod, now)
	if err != nil {
		return nil, err
	}

	return NewPartitionInstanceRing(newStaticPartitionRingReader(partitionsSubring), r.instancesRing, r.heartbeatTimeout), nil
}

type staticPartitionRingReader struct {
	ring *PartitionRing
}

func newStaticPartitionRingReader(ring *PartitionRing) staticPartitionRingReader {
	return staticPartitionRingReader{
		ring: ring,
	}
}

func (m staticPartitionRingReader) PartitionRing() *PartitionRing {
	return m.ring
}

// uniqueZonesFromInstances returns the unique list of zones among the input instances. The input buf MUST have
// zero length, but could be capacity in order to avoid memory allocations.
func uniqueZonesFromInstances(instances []InstanceDesc, buf []string) []string {
	for _, instance := range instances {
		if !slices.Contains(buf, instance.Zone) {
			buf = append(buf, instance.Zone)
		}
	}

	return buf
}
