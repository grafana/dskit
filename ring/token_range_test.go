package ring

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyInTokenRanges(t *testing.T) {
	ranges := []uint32{4, 8, 12, 16}

	require.False(t, KeyInTokenRanges(0, ranges))
	require.True(t, KeyInTokenRanges(4, ranges))
	require.True(t, KeyInTokenRanges(6, ranges))
	require.True(t, KeyInTokenRanges(8, ranges))
	require.False(t, KeyInTokenRanges(10, ranges))
	require.False(t, KeyInTokenRanges(20, ranges))
}
