package ring

import (
	"slices"
)

// resolveRingTokens returns the sorted tokens of all partitions and the partition owning each token.
func resolveRingTokens(desc PartitionRingDesc) (Tokens, map[Token]int32) {
	ringTokens := make(Tokens, 0, len(desc.Partitions)*optimalTokensPerInstance)
	partitionByToken := make(map[Token]int32, len(desc.Partitions)*optimalTokensPerInstance)
	for id := range desc.Partitions {
		tokens := resolvePartitionTokens(desc, id)
		ringTokens = append(ringTokens, tokens...)
		for _, token := range tokens {
			partitionByToken[Token(token)] = id
		}
	}
	slices.Sort(ringTokens)
	return ringTokens, partitionByToken
}

// resolvePartitionTokens returns the immutable tokens of a partition.
func resolvePartitionTokens(desc PartitionRingDesc, id int32) Tokens {
	return desc.Partitions[id].Tokens
}
