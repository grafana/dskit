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

	// The shuffleShard() function returns nil if the subring is equal to this ring.
	// We don't cache it in that case, since it was shortcut by shuffleShard().
	if subring == nil {
		return r, nil
	}

	r.shuffleShardCache.setSubring(identifier, size, subring)
	return subring, nil
}

// ShuffleShardWithLookback is like ShuffleShard() but the returned subring includes all instances
// that have been part of the identifier's shard since "now - lookbackPeriod".
//
// The returned subring be never used for write operations (read only).
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

	// The shuffleShard() function returns nil if the subring is equal to this ring.
	// We don't cache it in that case, since it was shortcut by shuffleShard().
	if subring == nil {
		return r, nil
	}

	r.shuffleShardCache.setSubringWithLookback(identifier, size, lookbackPeriod, now, subring)
	return subring, nil
}

func (r *PartitionRing) shuffleShard(identifier string, size int, lookbackPeriod time.Duration, now time.Time) (*PartitionRing, error) {
	// Nothing to do if the shard size is not smaller then the actual ring.
	if size <= 0 || size >= len(r.desc.Partitions) {
		return nil, nil
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
	for len(result) < size {
		start := searchToken(r.ringTokens, random.Uint32())
		iterations := 0

		found := false
		for p := start; !found && iterations < tokensCount; p++ {
			iterations++

			if p >= tokensCount {
				p %= tokensCount
			}

			pid, ok := r.tokenPartitions[Token(r.ringTokens[p])]
			if !ok {
				return nil, ErrInconsistentTokensInfo
			}

			// Ensure we select new partition.
			if _, ok := result[pid]; ok {
				continue
			}

			// Include found partition in the result.
			result[pid] = struct{}{}

			p, ok := r.desc.Partition(pid)
			if !ok {
				return nil, ErrInconsistentTokensInfo
			}

			// If this partition is inactive (read-only), or became active recently (based on lookback), we need to include more partitions.
			if !p.IsActive() || (lookbackPeriod > 0 && p.BecameActiveAfter(lookbackUntil)) {
				size++

				// If we now need to find all partitions, just return nil to indicate that.
				if size >= len(r.desc.Partitions) {
					return nil, nil
				}
			}

			found = true
		}

		// If we iterated over all tokens, and no new partition has been found, we can stop looking for more partitions.
		if !found {
			break
		}
	}

	return NewPartitionRing(r.desc.WithPartitions(result)), nil
}

func (r *PartitionRing) PartitionOwners() map[int32][]string {
	return r.partitionOwners
}

// PartitionsCount returns the number of partitions in the ring.
func (r *PartitionRing) PartitionsCount() int {
	return len(r.desc.Partitions)
}

// ActivePartitionIDs returns a list of all active partition IDs in the ring.
func (r *PartitionRing) ActivePartitionIDs() []int32 {
	ids := make([]int32, 0, len(r.desc.Partitions))

	for id := range r.desc.Partitions {
		ids = append(ids, id)
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
