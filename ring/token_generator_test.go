package ring

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRandomTokenGenerator_GenerateTokens(t *testing.T) {
	tokenGenerator := NewRandomTokenGenerator()
	tokens, _ := tokenGenerator.GenerateTokens(1000000, nil)

	dups := make(map[uint32]int)

	for ix, v := range tokens {
		if ox, ok := dups[v]; ok {
			t.Errorf("Found duplicate token %d, tokens[%d]=%d, tokens[%d]=%d", v, ix, tokens[ix], ox, tokens[ox])
		} else {
			dups[v] = ix
		}
	}
}

func TestRandomTokenGenerator_IgnoresOldTokens(t *testing.T) {
	tokenGenerator := NewRandomTokenGenerator()
	first, err := tokenGenerator.GenerateTokens(1000000, nil)
	require.NoError(t, err)
	second, err := tokenGenerator.GenerateTokens(1000000, first)
	require.NoError(t, err)

	dups := make(map[uint32]bool)

	for _, v := range first {
		dups[v] = true
	}

	for _, v := range second {
		if dups[v] {
			t.Fatal("GenerateTokens returned old token")
		}
	}
}

// GenerateTokens generates numTokens unique, random and sorted tokens for testing purposes.
func GenerateTokens(tokensCount int, takenTokens []uint32) Tokens {
	tokens, err := NewRandomTokenGenerator().GenerateTokens(tokensCount, takenTokens)
	if err != nil {
		return nil
	}
	return tokens
}
