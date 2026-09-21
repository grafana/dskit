package ring

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveRingTokens(t *testing.T) {
	desc := PartitionRingDesc{
		Partitions: map[int32]PartitionDesc{
			1: {Tokens: []uint32{1, 5, 8}},
			2: {Tokens: []uint32{3, 4, 9}},
		},
	}

	ringTokens, partitionByToken, err := resolveRingTokens(desc, DefaultPartitionRingOptions())
	require.NoError(t, err)

	assert.Equal(t, Tokens{1, 3, 4, 5, 8, 9}, ringTokens)
	assert.Equal(t, map[Token]int32{1: 1, 5: 1, 8: 1, 3: 2, 4: 2, 9: 2}, partitionByToken)
}
