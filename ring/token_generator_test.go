package ring

import "testing"

func TestRandomTokenGenerator_GenerateTokens(t *testing.T) {
	tokenGenerator := newRandomTokenGenerator()
	tokens := tokenGenerator.GenerateTokens(1000000, nil)

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
	tokenGenerator := newRandomTokenGenerator()
	first := tokenGenerator.GenerateTokens(1000000, nil)
	second := tokenGenerator.GenerateTokens(1000000, first)

	dups := make(map[uint32]bool)

	for _, v := range first {
		dups[v] = true
	}

	for _, v := range second {
		if dups[v] {
			t.Fatal("generateRandomTokens returned old token")
		}
	}
}

// generateRandomTokens generates numTokens unique, random and sorted tokens for testing purposes.
func generateRandomTokens(numTokens int, takenTokens []uint32) Tokens {
	return newRandomTokenGenerator().GenerateTokens(numTokens, takenTokens)
}
