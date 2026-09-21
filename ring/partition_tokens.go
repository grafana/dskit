package ring

import (
	"fmt"
	"slices"
)

// PartitionTokenGenerator derives sorted tokens from a partition ID.
// Returned tokens are immutable: callers must not change elements or sort them in place.
// Implementations must be safe for concurrent use.
type PartitionTokenGenerator interface {
	TokensFor(id int32) (Tokens, error)
}

// resolveRingTokens returns the sorted tokens of all partitions and the partition owning each token.
func resolveRingTokens(desc PartitionRingDesc, opts PartitionRingOptions) (Tokens, map[Token]int32, error) {
	ringTokens := make(Tokens, 0, len(desc.Partitions)*optimalTokensPerInstance)
	partitionByToken := make(map[Token]int32, len(desc.Partitions)*optimalTokensPerInstance)
	for id := range desc.Partitions {
		tokens, err := resolvePartitionTokens(desc, id, opts)
		if err != nil {
			return nil, nil, err
		}
		ringTokens = append(ringTokens, tokens...)
		for _, token := range tokens {
			partitionByToken[Token(token)] = id
		}
	}
	slices.Sort(ringTokens)
	return ringTokens, partitionByToken, nil
}

// resolvePartitionTokens returns the immutable tokens of a partition.
func resolvePartitionTokens(desc PartitionRingDesc, id int32, opts PartitionRingOptions) (Tokens, error) {
	partition := desc.Partitions[id]
	switch partition.TokenScheme {
	case PartitionTokensStored:
		// Use the stored tokens, even when empty.
		return partition.Tokens, nil
	case PartitionTokensSmt512:
		// The scheme is authoritative even when stored tokens are present.
	default:
		return nil, fmt.Errorf("cannot resolve tokens for partition %d: unknown token scheme %d", id, partition.TokenScheme)
	}
	if opts.TokenGenerator == nil {
		return nil, fmt.Errorf("cannot resolve tokens for partition %d: ring has no token generator", id)
	}
	tokens, err := opts.TokenGenerator.TokensFor(id)
	if err != nil {
		return nil, fmt.Errorf("cannot resolve tokens for partition %d: %w", id, err)
	}
	return tokens, nil
}

// generatePartitionTokens returns sorted, deterministic tokens for every ID from 0 through maxPartitionID.
// TODO: use it to fill the partition token table in the next commit.
func generatePartitionTokens(maxPartitionID int32) ([]Tokens, error) {
	if maxPartitionID < 0 {
		return nil, fmt.Errorf("partition ID must be non-negative, got %d", maxPartitionID)
	}
	generator := NewSpreadMinimizingTokenGeneratorForInstanceAndZoneID("", int(maxPartitionID), 0, false)
	tokensByID, err := generator.generateTokensByInstanceID()
	if err != nil {
		return nil, err
	}
	tokens := make([]Tokens, int(maxPartitionID)+1)
	for id := range tokens {
		tokens[id] = tokensByID[id]
		slices.Sort(tokens[id])
	}
	return tokens, nil
}
