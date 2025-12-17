package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShuffleShardCacheStorage(t *testing.T) {
	testCases := []struct {
		name    string
		storage shuffleShardCacheStorage[string]
	}{
		{"map", newMapCacheStorage[string]()},
		{"lru", mustNewLRUCacheStorage[string](t, 10)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := subringCacheKey{identifier: "test", shardSize: 3, lookbackPeriod: time.Hour}

			// Initially empty
			require.Equal(t, 0, tc.storage.len())
			_, ok := tc.storage.get(key)
			require.False(t, ok)

			// Set and get
			tc.storage.set(key, "value1")
			require.Equal(t, 1, tc.storage.len())
			val, ok := tc.storage.get(key)
			require.True(t, ok)
			require.Equal(t, "value1", val)

			// Overwrite
			tc.storage.set(key, "value2")
			require.Equal(t, 1, tc.storage.len())
			val, ok = tc.storage.get(key)
			require.True(t, ok)
			require.Equal(t, "value2", val)
		})
	}
}

func mustNewLRUCacheStorage[V any](t *testing.T, size int) *lruCacheStorage[V] {
	s, err := newLRUCacheStorage[V](size)
	require.NoError(t, err)
	return s
}
