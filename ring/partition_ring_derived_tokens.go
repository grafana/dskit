package ring

import (
	"errors"
	"flag"
	"sync/atomic"
)

// Partition tokens are a pure function of the partition ID: AddPartition has always generated them
// with the spread-minimizing generator, in zone 0, and nothing ever changes them. A partition
// therefore declares the scheme its tokens are generated with instead of gossiping ~2KB of them,
// and readers which need the tokens derive them.

const (
	// TokenSchemeSpreadMinimizing512 is the token scheme every partition has implicitly been using
	// since partition rings were introduced: 512 tokens from the spread-minimizing generator.
	TokenSchemeSpreadMinimizing512 = "smt-v1-512"

	// spreadMinimizingGeneratorV1 is the generator behind the "smt-v1-*" schemes. Schemes sharing a
	// generator share the memoized tokens: a scheme declaring fewer tokens is served the prefix of
	// the reserved set, which is exactly what GenerateTokens returns.
	spreadMinimizingGeneratorV1 = "smt-v1"
)

// partitionTokenScheme is the (generator, tokens count) pair a scheme name maps to. Keeping the
// count in the name makes validating a gossiped value a map lookup, instead of a bounds check on a
// number a peer chose.
type partitionTokenScheme struct {
	generator   string
	tokensCount int
}

var partitionTokenSchemes = map[string]partitionTokenScheme{
	TokenSchemeSpreadMinimizing512: {generator: spreadMinimizingGeneratorV1, tokensCount: optimalTokensPerInstance},
}

// DefaultMaxDerivedPartitionID is the highest partition ID whose tokens are derived, by default. The
// whole range is derived when a partition ring watcher starts and never grows afterwards, so a ring
// holding a higher partition ID is rejected.
const DefaultMaxDerivedPartitionID = 2048

var maxDerivedPartitionID = func() *atomic.Int32 {
	v := &atomic.Int32{}
	v.Store(DefaultMaxDerivedPartitionID)
	return v
}()

// RegisterPartitionTokensFlags registers the flags controlling how the tokens of the partitions
// declaring a token scheme are derived.
func RegisterPartitionTokensFlags(f *flag.FlagSet) {
	f.Int("partition-ring.max-derived-partition-id", 0, "Not implemented.")
}

// SetMaxDerivedPartitionID sets the highest partition ID whose tokens this process derives.
func SetMaxDerivedPartitionID(int) error {
	return errors.New("not implemented")
}

// WarmDerivedPartitionTokens derives and memoizes the tokens of every derivable partition, so that
// building the first ring doesn't pay for it.
func WarmDerivedPartitionTokens() error {
	return errors.New("not implemented")
}

// GenerateAllTokensUpTo returns the tokens of every instance ID from 0 to maxID, in the zone-less
// form partitions use.
func GenerateAllTokensUpTo(int32) (map[int32]Tokens, error) {
	return nil, errors.New("not implemented")
}
