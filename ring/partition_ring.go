package ring

import (
	"bytes"
	"container/list"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"time"

	shardUtil "github.com/grafana/dskit/ring/shard"
	"github.com/pkg/errors"
)

var (
	ErrNoActivePartitionFound    = errors.New("no active partition found")
	ErrInconsistentPartitionInfo = errors.New("inconsistent partition info")
)

type partition struct {
	id         int32
	validUntil int64
}

func (p *partition) isValid(timestamp int64) bool {
	return p.isActive() || p.validUntil > timestamp
}

func (p *partition) isActive() bool {
	return p.validUntil == 0
}

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

	// partitionsByToken is a map where they key is a registered token and the value is a list of partition objects
	// that currently correspond or previously corresponded to that token. Head of the list represents the current
	// active partition.
	partitionsByToken map[Token]*list.List

	// ownersByPartition is a map where the key is the partition ID and the value is a list of owner IDs.
	ownersByPartition map[int32][]string

	// shuffleShardCache is used to cache subrings generated with shuffle sharding.
	shuffleShardCache *partitionRingShuffleShardCache

	// activePartitionsCount is a saved count of active partitions to avoid recomputing it.
	activePartitionsCount int
}

func NewPartitionRing(desc PartitionRingDesc) *PartitionRing {
	partitionByToken := desc.partitionByToken()

	partitionsByToken := make(map[Token]*list.List, len(partitionByToken))
	for token, pid := range partitionByToken {
		l := list.New()
		l.PushFront(&partition{id: pid})
		partitionsByToken[token] = l
	}
	return &PartitionRing{
		desc:                  desc,
		ringTokens:            desc.tokens(),
		partitionsByToken:     partitionsByToken,
		ownersByPartition:     desc.ownersByPartition(),
		activePartitionsCount: desc.activePartitionsCount(),
		shuffleShardCache:     newPartitionRingShuffleShardCache(),
	}
}

func (r *PartitionRing) clearShuffleShardCache() {
	r.shuffleShardCache = newPartitionRingShuffleShardCache()
}

func (r *PartitionRing) activePartitionForToken(token Token) (int32, bool) {
	partitions, ok := r.partitionsByToken[token]
	if !ok {
		return 0, false
	}
	activePartition := partitions.Front().Value.(*partition)
	return activePartition.id, true
}

func (r *PartitionRing) cleanupExpiredPartitions(duration time.Duration) {
	validUntil := time.Now().Add(-duration).Unix()
	for _, partitions := range r.partitionsByToken {
		curr := partitions.Front()
		for curr != nil {
			next := curr.Next()
			currPartition, ok := curr.Value.(*partition)
			if ok && !currPartition.isValid(validUntil) {
				partitions.Remove(curr)
			}
			curr = next
		}
	}
}

// ActivePartitionForKey returns partition for the given key. Only active partitions are considered.
// Only one partition is returned: in other terms, the replication factor is always 1.
func (r *PartitionRing) ActivePartitionForKey(key uint32) (int32, Token, error) {
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

		partitionID, ok := r.activePartitionForToken(Token(token))
		if !ok {
			return 0, 0, ErrInconsistentTokensInfo
		}

		partition, ok := r.desc.Partitions[partitionID]
		if !ok {
			return 0, 0, ErrInconsistentTokensInfo
		}

		// If the partition is not active we'll keep walking the ring.
		if partition.IsActive() {
			return partitionID, Token(token), nil
		}
	}

	return 0, 0, ErrNoActivePartitionFound
}

// ShuffleShardSize returns number of partitions that would be in the result of ShuffleShard call with the same size.
func (r *PartitionRing) ShuffleShardSize(size int) int {
	if size <= 0 || size > r.activePartitionsCount {
		return r.activePartitionsCount
	}

	if size < r.activePartitionsCount {
		return size
	}
	return r.activePartitionsCount
}

// ShuffleShard returns a subring for the provided identifier (eg. a tenant ID)
// and size (number of partitions).
//
// The algorithm used to build the subring is a shuffle sharder based on probabilistic
// hashing. We pick N unique partitions, walking the ring starting from random but
// predictable numbers. The random generator is initialised with a seed based on the
// provided identifier.
//
// This function returns a subring containing ONLY ACTIVE partitions.
//
// This function supports caching.
//
// This implementation guarantees:
//
//   - Stability: given the same ring, two invocations returns the same result.
//
//   - Consistency: adding/removing 1 partition from the ring generates a resulting
//     subring with no more then 1 difference.
//
//   - Shuffling: probabilistically, for a large enough cluster each identifier gets a different
//     set of instances, with a reduced number of overlapping instances between two identifiers.
func (r *PartitionRing) ShuffleShard(identifier string, size int) (*PartitionRing, error) {
	if cached := r.shuffleShardCache.getSubring(identifier, size); cached != nil {
		return cached, nil
	}

	// No need to pass the time if there's no lookback.
	subring, err := r.shuffleShard(identifier, size, 0, time.Time{})
	if err != nil {
		return nil, err
	}

	r.shuffleShardCache.setSubring(identifier, size, subring)
	return subring, nil
}

// ShuffleShardWithLookback is like ShuffleShard() but the returned subring includes all instances
// that have been part of the identifier's shard in [now - lookbackPeriod, now] time window.
//
// This function can return a mix of ACTIVE and INACTIVE partitions. INACTIVE partitions are only
// included if they were part of the identifier's shard within the lookbackPeriod. PENDING partitions
// are never returned.
//
// This function supports caching, but the cache will only be effective if successive calls for the
// same identifier are with the same lookbackPeriod and increasing values of now.
func (r *PartitionRing) ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) (*PartitionRing, error) {
	if cached := r.shuffleShardCache.getSubringWithLookback(identifier, size, lookbackPeriod, now); cached != nil {
		return cached, nil
	}

	subring, err := r.shuffleShard(identifier, size, lookbackPeriod, now)
	if err != nil {
		return nil, err
	}

	r.shuffleShardCache.setSubringWithLookback(identifier, size, lookbackPeriod, now, subring)
	return subring, nil
}

func (r *PartitionRing) shuffleShard(identifier string, size int, lookbackPeriod time.Duration, now time.Time) (*PartitionRing, error) {
	// If the size is too small or too large, run with a size equal to the total number of partitions.
	// We have to run the function anyway because the logic may filter out some INACTIVE partitions.
	if size <= 0 || size >= len(r.desc.Partitions) {
		size = len(r.desc.Partitions)
	}

	var lookbackUntil int64
	if lookbackPeriod > 0 {
		lookbackUntil = now.Add(-lookbackPeriod).Unix()
	}

	// Initialise the random generator used to select instances in the ring.
	// There are no zones
	random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(identifier, "")))

	// To select one more instance while guaranteeing the "consistency" property,
	// we do pick a random value from the generator and resolve uniqueness collisions
	// (if any) continuing walking the ring.
	tokensCount := len(r.ringTokens)

	result := make(map[int32]struct{}, size)
	exclude := map[int32]struct{}{}

	for len(result) < size {
		start := searchToken(r.ringTokens, random.Uint32())
		iterations := 0
		found := false

		for p := start; !found && iterations < tokensCount; p++ {
			iterations++

			// Wrap p around in the ring.
			if p >= tokensCount {
				p %= tokensCount
			}

			partitions, ok := r.partitionsByToken[Token(r.ringTokens[p])]
			if !ok {
				return nil, ErrInconsistentTokensInfo
			}

			for e := partitions.Front(); e != nil; e = e.Next() {
				currentPartition, ok := e.Value.(*partition)
				if !ok {
					return nil, ErrInconsistentPartitionInfo
				}

				validUntil := now.Unix()
				if lookbackUntil > 0 {
					validUntil = now.Add(-lookbackPeriod).Unix()
				}
				if !currentPartition.isValid(validUntil) {
					continue
				}

				shouldExtend := !currentPartition.isActive()

				pid := currentPartition.id
				// Ensure the partition has not already been included or excluded.
				if _, ok := result[pid]; ok {
					continue
				}
				if _, ok := exclude[pid]; ok {
					continue
				}

				p, ok := r.desc.Partitions[pid]
				if !ok {
					return nil, ErrInconsistentPartitionInfo
				}

				// PENDING partitions should be skipped because they're not ready for read or write yet,
				// and they don't need to be looked back.
				if p.IsPending() {
					exclude[pid] = struct{}{}
					continue
				}

				var (
					withinLookbackPeriod = lookbackPeriod > 0 && p.GetStateTimestamp() >= lookbackUntil
					shouldInclude        = p.IsActive() || withinLookbackPeriod
				)

				shouldExtend = shouldExtend || withinLookbackPeriod

				// Either include or exclude the found partition.
				if shouldInclude {
					result[pid] = struct{}{}
				} else {
					exclude[pid] = struct{}{}
				}

				// Extend the shard, if requested.
				if shouldExtend {
					size++
				}

				// We can stop searching for other partitions only if this partition was included
				// and no extension was requested, which means it's the "stop partition" for this cycle.
				if shouldInclude && !shouldExtend {
					found = true
				}

			}
		}

		// If we iterated over all tokens, and no new partition has been found, we can stop looking for more partitions.
		if !found {
			break
		}
	}

	return NewPartitionRing(r.desc.WithPartitions(result)), nil
}

// PartitionsCount returns the number of partitions in the ring.
func (r *PartitionRing) PartitionsCount() int {
	return len(r.desc.Partitions)
}

// ActivePartitionsCount returns the number of active partitions in the ring.
func (r *PartitionRing) ActivePartitionsCount() int {
	return r.activePartitionsCount
}

// Partitions returns the partitions in the ring.
// The returned slice is a deep copy, so the caller can freely manipulate it.
func (r *PartitionRing) Partitions() []PartitionDesc {
	res := make([]PartitionDesc, 0, len(r.desc.Partitions))

	for _, partition := range r.desc.Partitions {
		res = append(res, partition.Clone())
	}

	return res
}

// PartitionIDs returns a sorted list of all partition IDs in the ring.
// The returned slice is a copy, so the caller can freely manipulate it.
func (r *PartitionRing) PartitionIDs() []int32 {
	ids := make([]int32, 0, len(r.desc.Partitions))

	for id := range r.desc.Partitions {
		ids = append(ids, id)
	}

	slices.Sort(ids)
	return ids
}

// PendingPartitionIDs returns a sorted list of all PENDING partition IDs in the ring.
// The returned slice is a copy, so the caller can freely manipulate it.
func (r *PartitionRing) PendingPartitionIDs() []int32 {
	ids := make([]int32, 0, len(r.desc.Partitions))

	for id, partition := range r.desc.Partitions {
		if partition.IsPending() {
			ids = append(ids, id)
		}
	}

	slices.Sort(ids)
	return ids
}

// ActivePartitionIDs returns a sorted list of all ACTIVE partition IDs in the ring.
// The returned slice is a copy, so the caller can freely manipulate it.
func (r *PartitionRing) ActivePartitionIDs() []int32 {
	ids := make([]int32, 0, len(r.desc.Partitions))

	for id, partition := range r.desc.Partitions {
		if partition.IsActive() {
			ids = append(ids, id)
		}
	}

	slices.Sort(ids)
	return ids
}

// InactivePartitionIDs returns a sorted list of all INACTIVE partition IDs in the ring.
// The returned slice is a copy, so the caller can freely manipulate it.
func (r *PartitionRing) InactivePartitionIDs() []int32 {
	ids := make([]int32, 0, len(r.desc.Partitions))

	for id, partition := range r.desc.Partitions {
		if partition.IsInactive() {
			ids = append(ids, id)
		}
	}

	slices.Sort(ids)
	return ids
}

// PartitionOwnerIDs returns a list of owner IDs for the given partitionID.
// The returned slice is NOT a copy and should be never modified by the caller.
func (r *PartitionRing) PartitionOwnerIDs(partitionID int32) (doNotModify []string) {
	return r.ownersByPartition[partitionID]
}

// PartitionOwnerIDsCopy is like PartitionOwnerIDs(), but the returned slice is a copy,
// so the caller can freely manipulate it.
func (r *PartitionRing) PartitionOwnerIDsCopy(partitionID int32) []string {
	ids := r.ownersByPartition[partitionID]
	if len(ids) == 0 {
		return nil
	}

	return slices.Clone(ids)
}

func (r *PartitionRing) String() string {
	buf := bytes.Buffer{}
	for pid, pd := range r.desc.Partitions {
		buf.WriteString(fmt.Sprintf(" %d:%v", pid, pd.State.String()))
	}

	return fmt.Sprintf("PartitionRing{ownersCount: %d, partitionsCount: %d, partitions: {%s}}", len(r.desc.Owners), len(r.desc.Partitions), buf.String())
}

// GetTokenRangesForPartition returns token-range owned by given partition. Note that this
// method does NOT take partition state into account, so if only active partitions should be
// considered, then PartitionRing with only active partitions must be created first (e.g. using ShuffleShard method).
func (r *PartitionRing) GetTokenRangesForPartition(partitionID int32) (TokenRanges, error) {
	partition, ok := r.desc.Partitions[partitionID]
	if !ok {
		return nil, ErrPartitionDoesNotExist
	}

	// 1 range (2 values) per token + one additional if we need to split the rollover range.
	ranges := make(TokenRanges, 0, 2*(len(partition.Tokens)+1))

	addRange := func(start, end uint32) {
		// check if we can group ranges. If so, we just update end of previous range.
		if len(ranges) > 0 && ranges[len(ranges)-1] == start-1 {
			ranges[len(ranges)-1] = end
		} else {
			ranges = append(ranges, start, end)
		}
	}

	// "last" range is range that includes token math.MaxUint32.
	ownsLastRange := false
	startOfLastRange := uint32(0)

	// We start with all tokens, but will remove tokens we already skipped, to let binary search do less work.
	ringTokens := r.ringTokens

	for iter, t := range partition.Tokens {
		lastOwnedToken := t - 1

		ix := searchToken(ringTokens, lastOwnedToken)
		prevIx := ix - 1

		if prevIx < 0 {
			// We can only find "last" range during first iteration.
			if iter > 0 {
				return nil, ErrInconsistentTokensInfo
			}

			prevIx = len(ringTokens) - 1
			ownsLastRange = true

			startOfLastRange = ringTokens[prevIx]

			// We can only claim token 0 if our actual token in the ring (which is exclusive end of range) was not 0.
			if t > 0 {
				addRange(0, lastOwnedToken)
			}
		} else {
			addRange(ringTokens[prevIx], lastOwnedToken)
		}

		// Reduce number of tokens we need to search through. We keep current token to serve as min boundary for next search,
		// to make sure we don't find another "last" range (where prevIx < 0).
		ringTokens = ringTokens[ix:]
	}

	if ownsLastRange {
		addRange(startOfLastRange, math.MaxUint32)
	}

	return ranges, nil
}

// ActivePartitionBatchRing wraps PartitionRing and implements DoBatchRing to lookup ACTIVE partitions.
type ActivePartitionBatchRing struct {
	ring *PartitionRing
}

func NewActivePartitionBatchRing(ring *PartitionRing) *ActivePartitionBatchRing {
	return &ActivePartitionBatchRing{
		ring: ring,
	}
}

// InstancesCount returns the number of active partitions in the ring.
//
// InstancesCount implements DoBatchRing.InstancesCount.
func (r *ActivePartitionBatchRing) InstancesCount() int {
	return r.ring.ActivePartitionsCount()
}

// ReplicationFactor returns 1 as partitions replication factor: an entry (looked by key via Get())
// is always stored in 1 and only 1 partition.
//
// ReplicationFactor implements DoBatchRing.ReplicationFactor.
func (r *ActivePartitionBatchRing) ReplicationFactor() int {
	return 1
}

// Get implements DoBatchRing.Get.
func (r *ActivePartitionBatchRing) Get(key uint32, _ Operation, bufInstances []InstanceDesc, _, _ []string) (ReplicationSet, error) {
	partitionID, _, err := r.ring.ActivePartitionForKey(key)
	if err != nil {
		return ReplicationSet{}, err
	}

	// Ensure we have enough capacity in bufInstances.
	if cap(bufInstances) < 1 {
		bufInstances = []InstanceDesc{{}}
	} else {
		bufInstances = bufInstances[:1]
	}

	partitionIDString := strconv.Itoa(int(partitionID))

	bufInstances[0] = InstanceDesc{
		Addr:      partitionIDString,
		Timestamp: 0,
		State:     ACTIVE,
		Id:        partitionIDString,
	}

	return ReplicationSet{
		Instances:            bufInstances,
		MaxErrors:            0,
		MaxUnavailableZones:  0,
		ZoneAwarenessEnabled: false,
	}, nil
}
