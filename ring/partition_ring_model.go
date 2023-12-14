package ring

import (
	"cmp"
	"fmt"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/exp/slices"

	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
)

type PartitionID int32

type PartitionState int

const (
	PartitionActive   PartitionState = 1
	PartitionInactive                = 2
	PartitionDeleted                 = 3 // This state is not visible to ring clients, it's only used for gossiping, and partitions in this state are removed before client can see them.
)

// State returns state of the partition. State is determined by highest state timestamp.
// If all timestamps are equal, partition is "active".
func (m *PartitionDesc) State() (PartitionState, int64) {
	if m.ActiveSince >= m.InactiveSince {
		if m.ActiveSince >= m.DeletedSince {
			// Active >= Inactive && Active >= Deleted.
			return PartitionActive, m.ActiveSince
		}

		// Deleted > Active >= Inactive.
		return PartitionDeleted, m.DeletedSince
	}

	// Inactive > Active.
	if m.InactiveSince >= m.DeletedSince {
		// Inactive > Active && Inactive >= Deleted
		return PartitionInactive, m.InactiveSince
	}

	// Deleted > Inactive > Active
	return PartitionDeleted, m.DeletedSince
}

func (m *PartitionDesc) NormalizeTimestamps() {
	s, _ := m.State()
	switch s {
	case PartitionActive:
		m.InactiveSince = 0
		m.DeletedSince = 0
	case PartitionInactive:
		m.ActiveSince = 0
		m.DeletedSince = 0
	case PartitionDeleted:
		m.ActiveSince = 0
		m.InactiveSince = 0
	}
}

func (m *PartitionDesc) IsActive() bool {
	s, _ := m.State()
	return s == PartitionActive
}

func (m *PartitionDesc) BecameActiveAfter(ts int64) bool {
	return m.IsActive() && m.ActiveSince >= ts
}

func (m *OwnerDesc) IsHealthy(op Operation, heartbeatTimeout time.Duration, now time.Time) bool {
	healthy := op.IsInstanceInStateHealthy(m.State)

	return healthy && IsHeartbeatHealthy(time.Unix(m.Heartbeat, 0), heartbeatTimeout, now)
}

func NewPartitionRingDesc() *PartitionRingDesc {
	return &PartitionRingDesc{
		Partitions: map[int32]PartitionDesc{},
		Owners:     map[string]OwnerDesc{},
	}
}

// PartitionRingDescFactory makes new Descs
func PartitionRingDescFactory() proto.Message {
	return NewPartitionRingDesc()
}

func GetPartitionRingCodec() codec.Codec {
	return codec.NewProtoCodec("partitionRingDesc", PartitionRingDescFactory)
}

// TokensAndTokenPartitions returns the list of tokens and a mapping of each token to its corresponding partition ID.
func (m *PartitionRingDesc) TokensAndTokenPartitions() (Tokens, map[Token]PartitionID) {
	allTokens := make(Tokens, 0, len(m.Partitions)*optimalTokensPerInstance)

	out := make(map[Token]PartitionID, len(m.Partitions)*optimalTokensPerInstance)
	for partitionID, partition := range m.Partitions {
		for _, token := range partition.Tokens {
			out[Token(token)] = PartitionID(partitionID)

			allTokens = append(allTokens, token)
		}
	}

	slices.Sort(allTokens)
	return allTokens, out
}

func (m *PartitionRingDesc) PartitionOwners() map[PartitionID][]string {
	out := make(map[PartitionID][]string, len(m.Partitions))
	for id, o := range m.Owners {
		pid := PartitionID(o.OwnedPartition)
		out[pid] = append(out[pid], id)
	}
	return out
}

func (m *PartitionRingDesc) AddActivePartition(id PartitionID, now time.Time) {
	// Spread-minimizing token generator is deterministic unique-token generator for given id and zone.
	// Partitions don't use zones.
	tg := NewSpreadMinimizingTokenGeneratorForInstanceAndZoneID("", int(id), 0, false)

	tokens := tg.GenerateTokens(optimalTokensPerInstance, nil)

	m.Partitions[int32(id)] = PartitionDesc{
		Tokens:      tokens,
		ActiveSince: now.Unix(),
	}
}

func (m *PartitionRingDesc) DisablePartition(id PartitionID, now time.Time) {
	d, ok := m.Partition(id)
	if ok {
		d.ActiveSince = 0
		d.InactiveSince = now.Unix()
		m.Partitions[int32(id)] = d
	}
}

func (m *PartitionRingDesc) ActivatePartition(id PartitionID, now time.Time) {
	d, ok := m.Partition(id)
	if ok {
		d.ActiveSince = now.Unix()
		d.InactiveSince = 0
		m.Partitions[int32(id)] = d
	}
}

func (m *PartitionRingDesc) Partition(id PartitionID) (PartitionDesc, bool) {
	p, ok := m.Partitions[int32(id)]
	return p, ok
}

// WithPartitions returns a new PartitionRingDesc with only the specified partitions and their owners included.
func (m *PartitionRingDesc) WithPartitions(partitions map[PartitionID]struct{}) PartitionRingDesc {
	newPartitions := make(map[int32]PartitionDesc, len(partitions))
	newOwners := make(map[string]OwnerDesc, len(partitions)*2) // assuming two owners per partition.

	for pid, p := range m.Partitions {
		if _, ok := partitions[PartitionID(pid)]; ok {
			newPartitions[pid] = p
		}
	}

	for oid, o := range m.Owners {
		pid := o.OwnedPartition
		if _, ok := partitions[PartitionID(pid)]; ok {
			newOwners[oid] = o
		}
	}

	return PartitionRingDesc{
		Partitions: newPartitions,
		Owners:     newOwners,
	}
}

func (m *PartitionRingDesc) AddOrUpdateOwner(id, address, zone string, ownedPartition int32, state InstanceState) {
	m.Owners[id] = OwnerDesc{
		Id:             id,
		Addr:           address,
		Zone:           zone,
		OwnedPartition: ownedPartition,
		Heartbeat:      time.Now().Unix(),
		State:          state,
	}
}

func (m *PartitionRingDesc) Merge(mergeable memberlist.Mergeable, localCAS bool) (memberlist.Mergeable, error) {
	if mergeable == nil {
		return nil, nil
	}

	other, ok := mergeable.(*PartitionRingDesc)
	if !ok {
		// This method only deals with non-nil rings.
		return nil, fmt.Errorf("expected *PartitionRingDesc, got %T", mergeable)
	}

	change := NewPartitionRingDesc()

	// Handle partitions.
	for pid, otherPart := range other.Partitions {
		changed := false

		thisPart, exists := m.Partitions[pid]
		if !exists {
			changed = true
			thisPart = otherPart
		} else {
			// We don't need to check for conflicts, because all partitions have unique tokens already -- since our token generator is deterministic.
			// The only problem could be that over time token generation algorithm changes and starts producing different tokens.
			// We can detect that by adding "token generator version" into the PartitionDesc, and then preserving tokens generated by latest version only.
			// We can solve this in the future, when needed.
			if len(thisPart.Tokens) == 0 && len(otherPart.Tokens) > 0 {
				thisPart.Tokens = otherPart.Tokens
				changed = true
			}

			oldState, oldTs := thisPart.State()

			// Merge timestamps, but only report "change" if state or ts changes.
			thisPart.ActiveSince = max(thisPart.ActiveSince, otherPart.ActiveSince)
			thisPart.InactiveSince = max(thisPart.InactiveSince, otherPart.InactiveSince)
			thisPart.DeletedSince = max(thisPart.DeletedSince, otherPart.DeletedSince)

			newState, newTs := thisPart.State()

			if oldState != newState || oldTs != newTs {
				changed = true
			}
		}

		if changed {
			thisPart.NormalizeTimestamps()

			m.Partitions[pid] = thisPart
			change.Partitions[pid] = thisPart
		}
	}

	if localCAS {
		// Let's mark all missing partitions in incoming change as deleted.
		// This breaks commutativity! But we only do it locally, not when gossiping with others.
		for pid, thisPart := range m.Partitions {
			_, exists := m.Partitions[pid]
			if !exists {
				// Partition was removed from the ring. We need to preserve it locally, but we set DeletedSince.
				thisPart.DeletedSince = time.Now().Unix()
				thisPart.ActiveSince = 0
				m.Partitions[pid] = thisPart
				change.Partitions[pid] = thisPart
			}
		}
	}

	// Now let's handle owners. Owners don't have tokens, which simplifies things compared to "normal" ring.
	for id, otherOwner := range other.Owners {
		thisOwner := m.Owners[id]

		// ting.Timestamp will be 0, if there was no such ingester in our version
		if otherOwner.Heartbeat > thisOwner.Heartbeat || (otherOwner.Heartbeat == thisOwner.Heartbeat && otherOwner.State == LEFT && thisOwner.State != LEFT) {
			m.Owners[id] = otherOwner
			change.Owners[id] = otherOwner
		}
	}

	if localCAS {
		// Mark all missing owners as deleted.
		// This breaks commutativity! But we only do it locally, not when gossiping with others.
		for id, thisOwner := range m.Owners {
			if _, ok := other.Owners[id]; !ok && thisOwner.State != LEFT {
				// missing, let's mark our ingester as LEFT
				thisOwner.State = LEFT
				// We are deleting entry "now", and should not keep old timestamp, because there may already be pending
				// message in the gossip network with newer timestamp (but still older than "now").
				// Such message would "resurrect" this deleted entry.
				thisOwner.Heartbeat = time.Now().Unix()

				m.Owners[id] = thisOwner
				change.Owners[id] = thisOwner
			}
		}
	}

	// If nothing changed, report nothing.
	if len(change.Partitions) == 0 && len(change.Owners) == 0 {
		return nil, nil
	}

	return change, nil
}

// Remove after switching to go 1.21.
func max[T cmp.Ordered](x T, y T) T {
	if x < y {
		return y
	}
	return x
}

func (m *PartitionRingDesc) MergeContent() []string {
	result := make([]string, len(m.Partitions)+len(m.Owners))

	// We're assuming that partition IDs and instance IDs are not colliding (ie. no instance is called "1").
	for pid := range m.Partitions {
		result = append(result, strconv.Itoa(int(pid)))
	}

	for id := range m.Owners {
		result = append(result, fmt.Sprintf(id))
	}
	return result
}

func (m *PartitionRingDesc) RemoveTombstones(limit time.Time) (total, removed int) {
	for pid, part := range m.Partitions {
		st, ts := part.State()
		if st == PartitionDeleted {
			if limit.IsZero() || time.Unix(ts, 0).Before(limit) {
				delete(m.Partitions, pid)
				removed++
			} else {
				total++
			}
		}
	}

	for n, ing := range m.Owners {
		if ing.State == LEFT {
			if limit.IsZero() || time.Unix(ing.Heartbeat, 0).Before(limit) {
				delete(m.Owners, n)
				removed++
			} else {
				total++
			}
		}
	}
	return
}

func (m *PartitionRingDesc) Clone() memberlist.Mergeable {
	return proto.Clone(m).(*PartitionRingDesc)
}
