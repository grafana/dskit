package ring

import (
	"math/rand"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokens_Serialization(t *testing.T) {
	tokens := make(Tokens, 512)
	for i := 0; i < 512; i++ {
		tokens = append(tokens, uint32(rand.Int31()))
	}

	b, err := tokens.Marshal()
	require.NoError(t, err)

	var unmarshaledTokens Tokens
	require.NoError(t, unmarshaledTokens.Unmarshal(b))
	require.Equal(t, tokens, unmarshaledTokens)
}

func TestTokens_Equals(t *testing.T) {
	tests := []struct {
		first    Tokens
		second   Tokens
		expected bool
	}{
		{
			first:    Tokens{},
			second:   Tokens{},
			expected: true,
		},
		{
			first:    Tokens{1, 2, 3},
			second:   Tokens{1, 2, 3},
			expected: true,
		},
		{
			first:    Tokens{1, 2, 3},
			second:   Tokens{3, 2, 1},
			expected: true,
		},
		{
			first:    Tokens{1, 2},
			second:   Tokens{1, 2, 3},
			expected: false,
		},
	}

	for _, c := range tests {
		assert.Equal(t, c.expected, c.first.Equals(c.second))
		assert.Equal(t, c.expected, c.second.Equals(c.first))
	}
}

func TestTokens_Equals_ShouldNotModifyInputs(t *testing.T) {
	first := Tokens{3, 1, 2}
	second := Tokens{2, 3, 1}

	require.True(t, first.Equals(second))
	require.Equal(t, Tokens{3, 1, 2}, first)
	require.Equal(t, Tokens{2, 3, 1}, second)
}

func TestTokens_Equals_ShouldBeSafeForConcurrentUse(t *testing.T) {
	shared := Tokens{5, 3, 4, 1, 2}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				require.True(t, shared.Equals(Tokens{1, 2, 3, 4, 5}))
			}
		}()
	}
	wg.Wait()
}

func TestLoadTokensFromFile_ShouldGuaranteeSortedTokens(t *testing.T) {
	tmpDir := t.TempDir()

	// Store tokens to file.
	orig := Tokens{1, 5, 3}
	require.NoError(t, orig.StoreToFile(filepath.Join(tmpDir, "tokens")))

	// Read back and ensure they're sorted.
	actual, err := LoadTokensFromFile(filepath.Join(tmpDir, "tokens"))
	require.NoError(t, err)
	assert.Equal(t, Tokens{1, 3, 5}, actual)
}
