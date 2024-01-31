package ring

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/exp/slices"

	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
)

func GetPartitionRingCodec() codec.Codec {
	return codec.NewProtoCodec("partitionRingDesc", PartitionRingDescFactory)
}

// PartitionRingDescFactory makes new PartitionRingDesc.
func PartitionRingDescFactory() proto.Message {
	return NewPartitionRingDesc()
}

func GetOrCreatePartitionRingDesc(in any) *PartitionRingDesc {
	if in == nil {
		return NewPartitionRingDesc()
	}

	return in.(*PartitionRingDesc)
}

func NewPartitionRingDesc() *PartitionRingDesc {
	return &PartitionRingDesc{
		Partitions: map[int32]PartitionDesc{},
		Owners:     map[string]OwnerDesc{},
	}
}

// tokens returns a sort list of tokens registered by all partitions.
func (m *PartitionRingDesc) tokens() Tokens {
	allTokens := make(Tokens, 0, len(m.Partitions)*optimalTokensPerInstance)

	for _, partition := range m.Partitions {
		allTokens = append(allTokens, partition.Tokens...)
	}

	slices.Sort(allTokens)
	return allTokens
}

// partitionByToken returns the a map where they key is a registered token and the value is ID of the partition
// that registered that token.
func (m *PartitionRingDesc) partitionByToken() map[Token]int32 {
	out := make(map[Token]int32, len(m.Partitions)*optimalTokensPerInstance)

	for partitionID, partition := range m.Partitions {
		for _, token := range partition.Tokens {
			out[Token(token)] = partitionID
		}
	}

	return out
}

// ownersByPartition returns a map where the key is the partition ID and the value is a list of owner IDs.
func (m *PartitionRingDesc) ownersByPartition() map[int32][]string {
	out := make(map[int32][]string, len(m.Partitions))
	for id, o := range m.Owners {
		out[o.OwnedPartition] = append(out[o.OwnedPartition], id)
	}
	return out
}

// countPartitionsByState returns a map containing the number of partitions by state.
func (m *PartitionRingDesc) countPartitionsByState() map[PartitionState]int {
	// Init the map to have to zero values for all states.
	out := make(map[PartitionState]int, len(PartitionState_value)-2)
	for _, state := range PartitionState_value {
		if PartitionState(state) == PartitionUnknown || PartitionState(state) == PartitionDeleted {
			continue
		}

		out[PartitionState(state)] = 0
	}

	for _, partition := range m.Partitions {
		out[partition.State]++
	}

	return out
}

// AddPartition adds a new partition to the ring. Tokens are auto-generated using the spread minimizing strategy
// which generates deterministic unique tokens.
func (m *PartitionRingDesc) AddPartition(id int32, state PartitionState, now time.Time) {
	// Spread-minimizing token generator is deterministic unique-token generator for given id and zone.
	// Partitions don't use zones.
	spreadMinimizing := NewSpreadMinimizingTokenGeneratorForInstanceAndZoneID("", int(id), 0, false)

	m.Partitions[id] = PartitionDesc{
		Tokens:         spreadMinimizing.GenerateTokens(optimalTokensPerInstance, nil),
		State:          state,
		StateTimestamp: now.Unix(),
	}
}

// UpdatePartitionState changes the state of a partition. Returns true if the state was changed,
// or false if the update was a no-op.
func (m *PartitionRingDesc) UpdatePartitionState(id int32, state PartitionState, now time.Time) bool {
	d, ok := m.Partitions[id]
	if !ok {
		return false
	}

	if d.State == state {
		return false
	}

	d.State = state
	d.StateTimestamp = now.Unix()
	m.Partitions[id] = d
	return true
}

// RemovePartition removes a partition.
func (m *PartitionRingDesc) RemovePartition(id int32) {
	delete(m.Partitions, id)
}

// HasPartition returns whether a partition exists.
func (m *PartitionRingDesc) HasPartition(id int32) bool {
	_, ok := m.Partitions[id]
	return ok
}

// AddOrUpdateOwner adds or updates a partition owner in the ring. Returns true, if the
// owner was added or updated, false if it was left unchanged.
func (m *PartitionRingDesc) AddOrUpdateOwner(id string, state OwnerState, ownedPartition int32, now time.Time) bool {
	prev, ok := m.Owners[id]
	updated := OwnerDesc{
		State:          state,
		OwnedPartition: ownedPartition,

		// Preserve the previous timestamp so that we'll NOT compare it.
		// Then, if we detect that the OwnerDesc should be updated, we'll
		// also update the UpdateTimestamp.
		UpdatedTimestamp: prev.UpdatedTimestamp,
	}

	if ok && prev.Equal(updated) {
		return false
	}

	updated.UpdatedTimestamp = now.Unix()
	m.Owners[id] = updated
	return true
}

// RemoveOwner removes a partition owner.
func (m *PartitionRingDesc) RemoveOwner(id string) {
	delete(m.Owners, id)
}

// HasOwner returns whether a owner exists.
func (m *PartitionRingDesc) HasOwner(id string) bool {
	_, ok := m.Owners[id]
	return ok
}

// Merge implements memberlist.Mergeable.
func (m *PartitionRingDesc) Merge(mergeable memberlist.Mergeable, localCAS bool) (memberlist.Mergeable, error) {
	return m.mergeWithTime(mergeable, localCAS, time.Now())
}

func (m *PartitionRingDesc) mergeWithTime(mergeable memberlist.Mergeable, localCAS bool, now time.Time) (memberlist.Mergeable, error) {
	if mergeable == nil {
		return nil, nil
	}

	other, ok := mergeable.(*PartitionRingDesc)
	if !ok {
		// This method only deals with non-nil rings.
		return nil, fmt.Errorf("expected *PartitionRingDesc, got %T", mergeable)
	}

	if other == nil {
		return nil, nil
	}

	change := NewPartitionRingDesc()

	// Handle partitions.
	for id, otherPart := range other.Partitions {
		changed := false

		thisPart, exists := m.Partitions[id]
		if !exists {
			changed = true
			thisPart = otherPart
		} else {
			// We expect tokens to be immutable, so we don't need to check for conflicts.
			//
			// If in the future we'll change the tokens generation algorithm and we'll have to handle migration to
			// a different set of tokens then we'll add the support. For example, we could add "token generation version"
			// to PartitionDesc and then preserve tokens generated by latest version only.
			if len(thisPart.Tokens) == 0 && len(otherPart.Tokens) > 0 {
				thisPart.Tokens = otherPart.Tokens
				changed = true
			}

			if otherPart.StateTimestamp > thisPart.StateTimestamp {
				changed = true

				thisPart.State = otherPart.State
				thisPart.StateTimestamp = otherPart.StateTimestamp
			}
		}

		if changed {
			m.Partitions[id] = thisPart
			change.Partitions[id] = thisPart
		}
	}

	if localCAS {
		// Let's mark all missing partitions in incoming change as deleted.
		// This breaks commutativity! But we only do it locally, not when gossiping with others.
		for pid, thisPart := range m.Partitions {
			if _, exists := other.Partitions[pid]; !exists && thisPart.State != PartitionDeleted {
				// Partition was removed from the ring. We need to preserve it locally, but we set state to PartitionDeleted.
				thisPart.State = PartitionDeleted
				thisPart.StateTimestamp = now.Unix()
				m.Partitions[pid] = thisPart
				change.Partitions[pid] = thisPart
			}
		}
	}

	// Now let's handle owners. Owners don't have tokens, which simplifies things compared to "normal" ring.
	for id, otherOwner := range other.Owners {
		thisOwner := m.Owners[id]

		if otherOwner.UpdatedTimestamp > thisOwner.UpdatedTimestamp || (otherOwner.UpdatedTimestamp == thisOwner.UpdatedTimestamp && otherOwner.State == OwnerDeleted && thisOwner.State != OwnerDeleted) {
			m.Owners[id] = otherOwner
			change.Owners[id] = otherOwner
		}
	}

	if localCAS {
		// Mark all missing owners as deleted.
		// This breaks commutativity! But we only do it locally, not when gossiping with others.
		for id, thisOwner := range m.Owners {
			if _, exists := other.Owners[id]; !exists && thisOwner.State != OwnerDeleted {
				// Owner was removed from the ring. We need to preserve it locally, but we set state to OwnerDeleted.
				thisOwner.State = OwnerDeleted
				thisOwner.UpdatedTimestamp = now.Unix()
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

// MergeContent implements memberlist.Mergeable.
func (m *PartitionRingDesc) MergeContent() []string {
	result := make([]string, len(m.Partitions)+len(m.Owners))

	// We're assuming that partition IDs and instance IDs are not colliding (ie. no instance is called "1").
	for pid := range m.Partitions {
		result = append(result, strconv.Itoa(int(pid)))
	}

	for id := range m.Owners {
		result = append(result, id)
	}
	return result
}

// RemoveTombstones implements memberlist.Mergeable.
func (m *PartitionRingDesc) RemoveTombstones(limit time.Time) (total, removed int) {
	for pid, part := range m.Partitions {
		if part.State == PartitionDeleted {
			if limit.IsZero() || time.Unix(part.StateTimestamp, 0).Before(limit) {
				delete(m.Partitions, pid)
				removed++
			} else {
				total++
			}
		}
	}

	for n, owner := range m.Owners {
		if owner.State == OwnerDeleted {
			if limit.IsZero() || time.Unix(owner.UpdatedTimestamp, 0).Before(limit) {
				delete(m.Owners, n)
				removed++
			} else {
				total++
			}
		}
	}

	return
}

// Clone implements memberlist.Mergeable.
func (m *PartitionRingDesc) Clone() memberlist.Mergeable {
	clone := proto.Clone(m).(*PartitionRingDesc)

	// Ensure empty maps are preserved (easier to compare with a deep equal in tests).
	if m.Partitions != nil && clone.Partitions == nil {
		clone.Partitions = map[int32]PartitionDesc{}
	}
	if m.Owners != nil && clone.Owners == nil {
		clone.Owners = map[string]OwnerDesc{}
	}

	return clone
}

func (m *PartitionDesc) IsActive() bool {
	return m.GetState() == PartitionActive
}

// CleanName returns the PartitionState name without the "Partition" prefix.
func (s PartitionState) CleanName() string {
	return strings.TrimPrefix(s.String(), "Partition")
}
