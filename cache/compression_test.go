package cache

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
)

func TestCompressionConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		cfg      CompressionConfig
		expected error
	}{
		"should pass with default config": {
			cfg: CompressionConfig{},
		},
		"should pass with snappy compression": {
			cfg: CompressionConfig{
				Compression: "snappy",
			},
		},
		"should fail with unsupported compression": {
			cfg: CompressionConfig{
				Compression: "unsupported",
			},
			expected: errUnsupportedCompression,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.Validate())
		})
	}
}

func TestSnappyCache(t *testing.T) {
	ctx := context.Background()
	backend := NewMockCache()
	c := NewSnappy(backend, log.NewNopLogger())

	t.Run("GetMulti() should return empty results if no key has been found", func(t *testing.T) {
		assert.Empty(t, c.GetMulti(ctx, []string{"a", "b", "c"}))
	})

	t.Run("GetMulti() should return previously set keys", func(t *testing.T) {
		expected := map[string][]byte{
			"a": []byte("value-a"),
			"b": []byte("value-b"),
		}

		c.SetMultiAsync(expected, time.Hour)
		assert.Equal(t, expected, c.GetMulti(ctx, []string{"a", "b", "c"}))
	})

	t.Run("GetMulti() should skip entries failing to decode", func(t *testing.T) {
		c.SetMultiAsync(map[string][]byte{"a": []byte("value-a")}, time.Hour)
		backend.SetMultiAsync(map[string][]byte{"b": []byte("value-b")}, time.Hour)

		expected := map[string][]byte{
			"a": []byte("value-a"),
		}
		assert.Equal(t, expected, c.GetMulti(ctx, []string{"a", "b", "c"}))
	})
}
