package ring

import (
	"fmt"
	"slices"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The tokens of a partition are a function of the partition ID only. AddPartition generates them
// with the spread-minimizing generator, which is hardcoded. A partition can therefore hold no
// tokens, and a reader derives the same tokens from the partition ID.
//
// Two settings control this: OmitTokens on a lifecycler creates partitions with no tokens, and a
// PartitionTokenGenerator on a ring derives them. A member that only relays the ring sets neither.

// DefaultMaxDerivedPartitionID is the usual upper bound for a PartitionTokenGenerator. A pass over
// this range takes approximately 400ms and keeps approximately 4MB. See BenchmarkGenerateAllTokensUpTo.
const DefaultMaxDerivedPartitionID = 2048

// PartitionTokenGenerator holds the tokens of the partitions 0 to maxPartitionID.
//
// The range does not change after construction. The generator derives the tokens one time and does
// not modify them again, so the rings of a process share them without a lock. Build one generator
// for each process, and give it to every watcher.
type PartitionTokenGenerator struct {
	maxPartitionID int32
	tokens         map[int32]Tokens
}

// NewPartitionTokenGenerator derives the tokens of the partitions 0 to maxPartitionID. The cost of
// the pass increases more than linearly with maxPartitionID, and a ring with a higher partition ID
// cannot be built. Set the range above the expected partition count.
func NewPartitionTokenGenerator(maxPartitionID int32, logger log.Logger, reg prometheus.Registerer) (*PartitionTokenGenerator, error) {
	if maxPartitionID < 0 {
		return nil, fmt.Errorf("max partition ID %d is negative", maxPartitionID)
	}

	startTime := time.Now()
	tokens, err := generateAllTokensUpTo(maxPartitionID)
	if err != nil {
		return nil, fmt.Errorf("deriving the tokens of the partitions up to %d: %w", maxPartitionID, err)
	}

	// The pass runs one time, so log its cost instead of putting one sample in a histogram.
	level.Info(logger).Log("msg", "derived partition tokens", "max_partition_id", maxPartitionID, "duration", time.Since(startTime))

	// Register the gauge only after a successful pass. A member that builds no generator then has no
	// derived tokens metric at all.
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "partition_ring_derived_tokens_max_partition_id",
		Help: "Highest partition ID the derived partition tokens have been generated for.",
	}).Set(float64(maxPartitionID))

	return &PartitionTokenGenerator{maxPartitionID: maxPartitionID, tokens: tokens}, nil
}

// MaxPartitionID returns the highest partition ID this generator serves.
func (g *PartitionTokenGenerator) MaxPartitionID() int32 {
	return g.maxPartitionID
}

func (g *PartitionTokenGenerator) forPartition(id int32) (Tokens, error) {
	if id < 0 || id > g.maxPartitionID {
		return nil, fmt.Errorf("partition %d is outside the derived partition ID range [0, %d]", id, g.maxPartitionID)
	}

	// A partition with no tokens owns no part of the ring and receives no writes. The range check
	// above makes this condition impossible, so return an error instead of an empty set.
	tokens := g.tokens[id]
	if len(tokens) == 0 {
		return nil, fmt.Errorf("no derived tokens for partition %d", id)
	}

	// Limit the capacity. An append must not write into storage that the other rings share.
	return tokens[:len(tokens):len(tokens)], nil
}

// materializeDerivedTokens replaces the tokens of every partition with the derived tokens. Without
// a generator it returns the desc unchanged, and the ring uses the tokens as received.
//
// It ignores the tokens that a partition holds, because the derived tokens are the only correct
// ones. AddPartition hardcodes the generator and the token count, so a pass over a partition that
// holds its tokens returns the same tokens. A partition with different tokens is therefore
// corrected, not rejected. Only a partition ID outside the derived range is an error, and this
// includes a partition that holds its tokens.
//
// It does not modify the input map. PartitionRing keeps its desc as an immutable snapshot, and the
// caller, the KV store and the earlier rings share that map. The operation is idempotent, so
// shuffle sharding can build a sub-ring from a desc that already holds derived tokens.
func materializeDerivedTokens(desc PartitionRingDesc, generator *PartitionTokenGenerator) (PartitionRingDesc, error) {
	if generator == nil || len(desc.Partitions) == 0 {
		return desc, nil
	}

	materialized := make(map[int32]PartitionDesc, len(desc.Partitions))

	for id, partition := range desc.Partitions {
		tokens, err := generator.forPartition(id)
		if err != nil {
			return PartitionRingDesc{}, err
		}

		partition.Tokens = tokens
		materialized[id] = partition
	}

	return PartitionRingDesc{Partitions: materialized, Owners: desc.Owners}, nil
}

// generateAllTokensUpTo derives the tokens of the partitions 0 to maxID in one pass. A pass for one
// partition also computes the tokens of all lower IDs, so many single-partition passes are quadratic.
func generateAllTokensUpTo(maxID int32) (map[int32]Tokens, error) {
	generator := NewSpreadMinimizingTokenGeneratorForInstanceAndZoneID("", int(maxID), 0, false)

	tokensByID, err := generator.generateTokensByInstanceID()
	if err != nil {
		return nil, err
	}

	tokens := make(map[int32]Tokens, len(tokensByID))
	for id, instanceTokens := range tokensByID {
		slices.Sort(instanceTokens)
		tokens[int32(id)] = instanceTokens
	}

	return tokens, nil
}
