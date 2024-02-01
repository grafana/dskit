package ring

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/exp/slices"

	shardUtil "github.com/grafana/dskit/ring/shard"
)

var ErrNoActivePartitionFound = fmt.Errorf("no active partition found")

type PartitionRing struct {
	desc PartitionRingDesc

	ringTokens      Tokens
	tokenPartitions map[Token]int32
	partitionOwners map[int32][]string

	shuffleShardCache *partitionRingShuffleShardCache
}

func NewPartitionRing(desc PartitionRingDesc) *PartitionRing {
	tokens, tokenPartitions := desc.TokensAndTokenPartitions()

	pr := PartitionRing{
		desc:              desc,
		ringTokens:        tokens,
		tokenPartitions:   tokenPartitions,
		partitionOwners:   desc.PartitionOwners(),
		shuffleShardCache: newPartitionRingShuffleShardCache(),
	}
	return &pr
}

// ActivePartitionForKey returns partition that should receive given key. Only active partitions are considered,
// and only one partition is returned.
func (r *PartitionRing) ActivePartitionForKey(key uint32) (int32, PartitionDesc, error) {
	start := searchToken(r.ringTokens, key)
	iterations := 0

	tokensCount := len(r.ringTokens)
	for i := start; iterations < len(r.ringTokens); i++ {
		iterations++

		if i >= tokensCount {
			i %= len(r.ringTokens)
		}

		token := r.ringTokens[i]

		pid, ok := r.tokenPartitions[Token(token)]
		if !ok {
			return 0, PartitionDesc{}, ErrInconsistentTokensInfo
		}

		p, ok := r.desc.Partition(pid)
		if !ok {
			return 0, PartitionDesc{}, ErrInconsistentTokensInfo
		}

		if p.IsActive() {
			return pid, p, nil
		}
	}
	return 0, PartitionDesc{}, ErrNoActivePartitionFound
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

	subring, err := r.shuffleShard(identifier, size, 0, time.Now())
	if err != nil {
		return nil, err
	}

	r.shuffleShardCache.setSubring(identifier, size, subring)
	return subring, nil
}

// ShuffleShardWithLookback is like ShuffleShard() but the returned subring includes all instances
// that have been part of the identifier's shard since "now - lookbackPeriod".
//
// This function can return a mix of ACTIVE and INACTIVE partitions. INACTIVE partitions are only
// included if they were part of the identifier's shard within the lookbackPeriod.
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

	lookbackUntil := now.Add(-lookbackPeriod).Unix()

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

			pid, ok := r.tokenPartitions[Token(r.ringTokens[p])]
			if !ok {
				return nil, ErrInconsistentTokensInfo
			}

			// Ensure the partition has not already been included or excluded.
			if _, ok := result[pid]; ok {
				continue
			}
			if _, ok := exclude[pid]; ok {
				continue
			}

			p, ok := r.desc.Partition(pid)
			if !ok {
				return nil, ErrInconsistentTokensInfo
			}

			var (
				shouldInclude = false
				shouldExtend  = false
			)

			// Check whether the partition should be included in the result and/or the result set extended.
			if lookbackPeriod > 0 {
				if p.IsActive() {
					// The partition is active. We should include it. The result set is extended only
					// if the partition became active within the lookback window.
					shouldInclude = true
					shouldExtend = p.GetStateTimestamp() >= lookbackUntil
				} else {
					// The partition is inactive. We should include it (and then extend the result set)
					// only if became inactive within the lookback window.
					shouldInclude = p.GetStateTimestamp() >= lookbackUntil
					shouldExtend = shouldInclude
				}
			} else {
				// No lookback was provided. We only include active partitions and don't lookback.
				shouldInclude = p.IsActive()
				shouldExtend = false
			}

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

		// If we iterated over all tokens, and no new partition has been found, we can stop looking for more partitions.
		if !found {
			break
		}
	}

	return NewPartitionRing(r.desc.WithPartitions(result)), nil
}

func (r *PartitionRing) PartitionOwners() map[int32][]string {
	// TODO returning by reference is risky
	return r.partitionOwners
}

// PartitionsCount returns the number of partitions in the ring.
func (r *PartitionRing) PartitionsCount() int {
	return len(r.desc.Partitions)
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

// PartitionIDs returns a list of all partition IDs in the ring.
// The returned slice is a copy, so the caller can freely manipulate it.
func (r *PartitionRing) PartitionIDs() []int32 {
	ids := make([]int32, 0, len(r.desc.Partitions))

	for id := range r.desc.Partitions {
		ids = append(ids, id)
	}

	slices.Sort(ids)
	return ids
}

// ActivePartitionIDs returns a list of all ACTIVE partition IDs in the ring.
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

// InactivePartitionIDs returns a list of all INACTIVE partition IDs in the ring.
// The returned slice is a copy, so the caller can freely manipulate it.
func (r *PartitionRing) InactivePartitionIDs() []int32 {
	ids := make([]int32, 0, len(r.desc.Partitions))

	for id, partition := range r.desc.Partitions {
		if !partition.IsActive() {
			ids = append(ids, id)
		}
	}

	slices.Sort(ids)
	return ids
}

func (r *PartitionRing) String() string {
	buf := bytes.Buffer{}
	for pid, pd := range r.desc.Partitions {
		buf.WriteString(fmt.Sprintf(" %d:%v", pid, pd.State.String()))
	}

	return fmt.Sprintf("PartitionRing{ownersCount: %d, partitionsCount: %d, partitions: {%s}}", len(r.desc.Owners), len(r.desc.Owners), buf.String())
}
