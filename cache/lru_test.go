package cache

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestLRUCache_StoreFetchDelete(t *testing.T) {
	var (
		mock = NewMockCache()
		ctx  = context.Background()
	)
	// This entry is only known by our underlying cache.
	mock.SetMultiAsync(map[string][]byte{"buzz": []byte("buzz")}, time.Hour)

	reg := prometheus.NewPedanticRegistry()
	lru, err := WrapWithLRUCache(mock, "test", reg, 10000, 2*time.Hour)
	require.NoError(t, err)

	lru.SetMultiAsync(map[string][]byte{
		"foo": []byte("bar"),
		"bar": []byte("baz"),
	}, time.Minute)

	lru.SetMultiAsync(map[string][]byte{
		"expired": []byte("expired"),
	}, -time.Minute)

	result := lru.GetMulti(ctx, []string{"buzz", "foo", "bar", "expired"})
	require.Equal(t, map[string][]byte{
		"buzz": []byte("buzz"),
		"foo":  []byte("bar"),
		"bar":  []byte("baz"),
	}, result)

	// Ensure we cache back entries from the underlying cache.
	item, ok := lru.lru.Get("buzz")
	require.True(t, ok)
	require.Equal(t, []byte("buzz"), item.Data)
	require.True(t, time.Until(item.ExpiresAt) > 1*time.Hour)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cache_memory_items_count Total number of items currently in the in-memory cache.
		# TYPE cache_memory_items_count gauge
		cache_memory_items_count{name="test"} 3
		# HELP cache_memory_hits_total Total number of requests to the in-memory cache that were a hit.
		# TYPE cache_memory_hits_total counter
		cache_memory_hits_total{name="test"} 2
		# HELP cache_memory_requests_total Total number of requests to the in-memory cache.
		# TYPE cache_memory_requests_total counter
		cache_memory_requests_total{name="test"} 4
	`)))

	err = lru.Delete(context.Background(), "buzz")
	require.NoError(t, err)
	value := lru.GetMulti(context.Background(), []string{"buzz"})
	require.Equal(t, map[string][]uint8{}, value)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cache_memory_items_count Total number of items currently in the in-memory cache.
		# TYPE cache_memory_items_count gauge
		cache_memory_items_count{name="test"} 2
		# HELP cache_memory_hits_total Total number of requests to the in-memory cache that were a hit.
		# TYPE cache_memory_hits_total counter
		cache_memory_hits_total{name="test"} 2
		# HELP cache_memory_requests_total Total number of requests to the in-memory cache.
		# TYPE cache_memory_requests_total counter
		cache_memory_requests_total{name="test"} 5
	`)))
}

func TestLRUCache_Evictions(t *testing.T) {
	const maxItems = 2

	reg := prometheus.NewPedanticRegistry()
	lru, err := WrapWithLRUCache(NewMockCache(), "test", reg, maxItems, 2*time.Hour)
	require.NoError(t, err)

	lru.SetMultiAsync(map[string][]byte{
		"key_1": []byte("value_1"),
		"key_2": []byte("value_2"),
		"key_3": []byte("value_3"),
	}, time.Minute)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cache_memory_items_count Total number of items currently in the in-memory cache.
		# TYPE cache_memory_items_count gauge
		cache_memory_items_count{name="test"} 2
	`), "cache_memory_items_count"))
}

func TestLRUCache_SetAdd(t *testing.T) {
	const maxItems = 10

	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	lru, err := WrapWithLRUCache(NewMockCache(), "test", reg, maxItems, 2*time.Hour)
	require.NoError(t, err)

	// Trying to .Add() a key that already exists should result in an error
	require.NoError(t, lru.Set(ctx, "key_1", []byte("value_1"), time.Minute))
	require.NoError(t, lru.Set(ctx, "key_2", []byte("value_2"), time.Minute))
	require.NoError(t, lru.Set(ctx, "key_3", []byte("value_3"), time.Minute))
	require.ErrorIs(t, lru.Add(ctx, "key_1", []byte("value_1_2"), time.Minute), ErrNotStored)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cache_memory_items_count Total number of items currently in the in-memory cache.
		# TYPE cache_memory_items_count gauge
		cache_memory_items_count{name="test"} 3
	`), "cache_memory_items_count"))

	result := lru.GetMulti(ctx, []string{"key_1", "key_2", "key_3"})
	require.Equal(t, map[string][]byte{
		"key_1": []byte("value_1"),
		"key_2": []byte("value_2"),
		"key_3": []byte("value_3"),
	}, result)

	// Ensure we cache back entries from the underlying cache.
	item, ok := lru.lru.Get("key_1")
	require.True(t, ok, "expected to fetch %s from inner LRU cache, got %+v", "key_1", item)
	require.Equal(t, []byte("value_1"), item.Data)
}
