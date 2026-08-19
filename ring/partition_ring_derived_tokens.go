package ring

import (
	"fmt"
	"slices"
)

// Partition tokens are a pure function of the partition ID, so a partition can declare the scheme
// they come from instead of gossiping ~2KB of them. Validation is split from resolution: Decode
// checks that a partition can be resolved, and building a ring resolves it, so members which only
// relay the ring never run the generator.

// TokenSchemeSpreadMinimizing512 identifies 512 tokens from the v1 spread-minimizing generator.
const TokenSchemeSpreadMinimizing512 = "smt-v1-512"

// DefaultMaxDerivedPartitionID is the conventional upper bound for a PartitionTokenGenerator.
// Deriving this range costs ~400ms and ~4MB retained; see BenchmarkGenerateAllTokensUpTo.
const DefaultMaxDerivedPartitionID = 2048

// PartitionTokenGenerator resolves tokens for partitions 0 through maxPartitionID. It is safe for
// concurrent use.
type PartitionTokenGenerator struct {
	maxPartitionID int32
	tokens         map[int32]Tokens
}

// NewPartitionTokenGenerator derives tokens for partitions 0 through maxPartitionID.
func NewPartitionTokenGenerator(maxPartitionID int32) (*PartitionTokenGenerator, error) {
	if maxPartitionID < 0 {
		return nil, fmt.Errorf("max partition ID %d is negative", maxPartitionID)
	}

	tokens, err := generateAllTokensUpTo(maxPartitionID)
	if err != nil {
		return nil, fmt.Errorf("deriving the tokens of the partitions up to %d: %w", maxPartitionID, err)
	}

	return &PartitionTokenGenerator{maxPartitionID: maxPartitionID, tokens: tokens}, nil
}

func (g *PartitionTokenGenerator) forPartition(id int32, tokenScheme string) (Tokens, error) {
	if tokenScheme == "" {
		return nil, fmt.Errorf("partition %d declares no token scheme", id)
	}

	if err := validatePartitionTokenScheme(tokenScheme); err != nil {
		return nil, fmt.Errorf("partition %d declares %w", id, err)
	}

	if id < 0 || id > g.maxPartitionID {
		return nil, fmt.Errorf("partition %d declares token scheme %q but its ID is outside the served partition ID range [0, %d]", id, tokenScheme, g.maxPartitionID)
	}

	// A partition resolved to no tokens owns nothing, and would silently stop receiving the writes
	// routed to it. The range check above makes this unreachable, so fail loudly rather than resolve
	// to an empty set.
	tokens := g.tokens[id]
	if len(tokens) == 0 {
		return nil, fmt.Errorf("no derived tokens for partition %d", id)
	}

	// Prevent append from overwriting storage shared with other rings.
	return tokens[:len(tokens):len(tokens)], nil
}

// validatePartitionTokenScheme accepts an empty scheme, which means a partition carries its tokens.
func validatePartitionTokenScheme(tokenScheme string) error {
	if tokenScheme != "" && tokenScheme != TokenSchemeSpreadMinimizing512 {
		return fmt.Errorf("unknown token scheme %q", tokenScheme)
	}

	return nil
}

// validatePartitionTokens requires a partition to either carry its tokens or declare the scheme to
// derive them from, never both and never neither.
func validatePartitionTokens(id int32, partition PartitionDesc) error {
	if partition.TokenScheme == "" {
		if len(partition.Tokens) == 0 {
			return fmt.Errorf("partition %d carries neither tokens nor a token scheme: the value was likely relayed by a binary predating the token scheme, which drops it", id)
		}
		return nil
	}

	if err := validatePartitionTokenScheme(partition.TokenScheme); err != nil {
		return fmt.Errorf("partition %d declares %w", id, err)
	}
	if len(partition.Tokens) > 0 {
		return fmt.Errorf("partition %d carries tokens and declares token scheme %q", id, partition.TokenScheme)
	}

	return nil
}

func validatePartitionRingTokens(desc *PartitionRingDesc) error {
	for id, partition := range desc.Partitions {
		if err := validatePartitionTokens(id, partition); err != nil {
			return err
		}
	}

	return nil
}

// materializeDerivedTokens resolves every partition declaring a token scheme, clearing the scheme so
// that the result is indistinguishable from a desc whose partitions carried their tokens. That's what
// makes this idempotent: shuffle sharding rebuilds sub-rings out of an already materialized desc.
//
// The input partitions map is never modified, since PartitionRing documents its desc as an immutable
// snapshot and the map is shared with the caller.
func materializeDerivedTokens(desc PartitionRingDesc, generator *PartitionTokenGenerator) (PartitionRingDesc, error) {
	needsTokens := false
	for id, partition := range desc.Partitions {
		if partition.TokenScheme != "" {
			if err := validatePartitionTokens(id, partition); err != nil {
				return PartitionRingDesc{}, err
			}
			needsTokens = true
		}
	}

	if !needsTokens {
		return desc, nil
	}

	materialized := PartitionRingDesc{
		Partitions: make(map[int32]PartitionDesc, len(desc.Partitions)),
		Owners:     desc.Owners,
	}

	for id, partition := range desc.Partitions {
		if partition.TokenScheme != "" {
			if generator == nil {
				return PartitionRingDesc{}, fmt.Errorf("partition %d declares token scheme %q but the ring is built without a partition token generator: only a PartitionRingWatcher given one via WithPartitionTokenGenerator can build a ring holding it", id, partition.TokenScheme)
			}

			tokens, err := generator.forPartition(id, partition.TokenScheme)
			if err != nil {
				return PartitionRingDesc{}, err
			}

			partition.Tokens = tokens
			partition.TokenScheme = ""
		}

		materialized.Partitions[id] = partition
	}

	return materialized, nil
}

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
