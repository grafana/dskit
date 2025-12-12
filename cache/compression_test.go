package cache

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
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

	t.Run("GetMultiWithError() should return results and nil error on success", func(t *testing.T) {
		backend := NewMockCache()
		c := NewSnappy(backend, log.NewNopLogger())

		expected := map[string][]byte{
			"a": []byte("value-a"),
			"b": []byte("value-b"),
		}
		c.SetMultiAsync(expected, time.Hour)
		result, err := c.GetMultiWithError(ctx, []string{"a", "b", "c"})
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("GetMultiWithError() should return error for invalid snappy data", func(t *testing.T) {
		backend := NewMockCache()
		c := NewSnappy(backend, log.NewNopLogger())

		// Set valid snappy-encoded value via SnappyCache
		c.SetMultiAsync(map[string][]byte{"valid": []byte("valid-value")}, time.Hour)

		// Set invalid (non-snappy) value directly to backend
		backend.SetMultiAsync(map[string][]byte{"invalid": []byte("not-snappy-encoded")}, time.Hour)

		result, err := c.GetMultiWithError(ctx, []string{"valid", "invalid"})

		// Should return the valid entry
		assert.Equal(t, map[string][]byte{"valid": []byte("valid-value")}, result)

		// Should return an error for the invalid entry
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode cache entry for key invalid")
	})

	t.Run("GetMulti() should log errors for snappy decoding", func(t *testing.T) {
		backend := NewMockCache()
		// Use a logger that writes to a buffer we can inspect.
		buffer := bytes.NewBuffer(nil)
		logger := log.NewLogfmtLogger(buffer)

		c := NewSnappy(backend, logger)

		// Set valid snappy-encoded value via SnappyCache
		c.SetMultiAsync(map[string][]byte{"valid": []byte("valid-value")}, time.Hour)

		// Set invalid (non-snappy) value directly to backend
		backend.SetMultiAsync(map[string][]byte{"invalid": []byte("not-snappy-encoded")}, time.Hour)

		result := c.GetMulti(ctx, []string{"valid", "invalid"})

		// Should return the valid entry
		assert.Equal(t, map[string][]byte{"valid": []byte("valid-value")}, result)

		// The log buffer should contain an error about the invalid entry
		logs := buffer.String()
		assert.Contains(t, logs, "failed to decode cache entry")
	})

	t.Run("GetMultiWithError() should propagate backend errors", func(t *testing.T) {
		backendErr := errors.New("backend error")
		backend := NewErroringMockCache(backendErr)
		c := NewSnappy(backend, log.NewNopLogger())

		// Store a valid snappy-encoded value directly in the backend
		backend.SetMultiAsync(map[string][]byte{"key": snappy.Encode(nil, []byte("value"))}, time.Hour)

		result, err := c.GetMultiWithError(ctx, []string{"key"})

		// Should return the decoded value
		assert.Equal(t, map[string][]byte{"key": []byte("value")}, result)

		// Should propagate the backend error
		assert.ErrorIs(t, err, backendErr)
	})
}
