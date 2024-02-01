package ring

import (
	"bytes"
	"fmt"
)

var ErrNoActivePartitionFound = fmt.Errorf("no active partition found")

// PartitionRing holds an immutable view of the partitions ring.
//
// Design principles:
//   - Immutable: the PartitionRingDesc hold by PartitionRing is immutable. When PartitionRingDesc changes
//     a new instance of PartitionRing should be created. The  partitions ring is expected to change infrequently
//     (e.g. there's no heartbeat), so creating a new PartitionRing each time the partitions ring changes is
//     not expected to have a significant overhead.
type PartitionRing struct {
	// desc is a snapshot of the partition ring. This data is immutable and MUST NOT be modified.
	desc PartitionRingDesc

	// ringTokens is a sorted list of all tokens registered by all partitions.
	ringTokens Tokens

	// partitionByToken is a map where they key is a registered token and the value is ID of the partition
	// that registered that token.
	partitionByToken map[Token]int32

	// ownersByPartition is a map where the key is the partition ID and the value is a list of owner IDs.
	ownersByPartition map[int32][]string
}

func NewPartitionRing(desc PartitionRingDesc) *PartitionRing {
	return &PartitionRing{
		desc:              desc,
		ringTokens:        desc.tokens(),
		partitionByToken:  desc.partitionByToken(),
		ownersByPartition: desc.ownersByPartition(),
	}
}

// ActivePartitionForKey returns partition for the given key. Only active partitions are considered.
// Only one partition is returned: in other terms, the replication factor is always 1.
func (r *PartitionRing) ActivePartitionForKey(key uint32) (int32, error) {
	var (
		start       = searchToken(r.ringTokens, key)
		iterations  = 0
		tokensCount = len(r.ringTokens)
	)

	for i := start; iterations < tokensCount; i++ {
		iterations++

		if i >= tokensCount {
			i %= len(r.ringTokens)
		}

		token := r.ringTokens[i]

		partitionID, ok := r.partitionByToken[Token(token)]
		if !ok {
			return 0, ErrInconsistentTokensInfo
		}

		partition, ok := r.desc.Partitions[partitionID]
		if !ok {
			return 0, ErrInconsistentTokensInfo
		}

		// If the partition is not active we'll keep walking the ring.
		if partition.IsActive() {
			return partitionID, nil
		}
	}

	return 0, ErrNoActivePartitionFound
}

// PartitionsCount returns the number of partitions in the ring.
func (r *PartitionRing) PartitionsCount() int {
	return len(r.desc.Partitions)
}

func (r *PartitionRing) String() string {
	buf := bytes.Buffer{}
	for pid, pd := range r.desc.Partitions {
		buf.WriteString(fmt.Sprintf(" %d:%v", pid, pd.State.String()))
	}

	return fmt.Sprintf("PartitionRing{ownersCount: %d, partitionsCount: %d, partitions: {%s}}", len(r.desc.Owners), len(r.desc.Partitions), buf.String())
}
