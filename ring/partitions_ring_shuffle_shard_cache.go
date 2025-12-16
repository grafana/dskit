package ring

import (
	"fmt"
	"math"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// partitionRingShuffleShardCacheStorage is the interface for the underlying storage mechanism.
// This abstracts the difference between map-based and LRU-based storage.
type partitionRingShuffleShardCacheStorage interface {
	get(key subringCacheKey) (*PartitionRing, bool)
	set(key subringCacheKey, value *PartitionRing)
	getWithLookback(key subringCacheKey) (cachedSubringWithLookback[*PartitionRing], bool)
	setWithLookback(key subringCacheKey, value cachedSubringWithLookback[*PartitionRing])
	lenWithLookback() int
	lenWithoutLookback() int
}

// partitionRingShuffleShardCache delegates storage operations to a cache storage implementation.
type partitionRingShuffleShardCache struct {
	cache partitionRingShuffleShardCacheStorage
}

// newPartitionRingShuffleShardCache creates a new partition ring shuffle shard cache.
// If size <= 0 an unbounded map-based cache is used.
// If size > 0, an LRU cache with the specified size is used.
func newPartitionRingShuffleShardCache(size int) (*partitionRingShuffleShardCache, error) {
	var cache partitionRingShuffleShardCacheStorage
	if size > 0 {
		lruCache, err := newPartitionRingLRUStorage(size)
		if err != nil {
			return nil, err
		}
		cache = lruCache
	} else {
		cache = newPartitionRingMapCache()
	}
	return &partitionRingShuffleShardCache{
		cache: cache,
	}, nil
}

func (r *partitionRingShuffleShardCache) setSubring(identifier string, size int, subring *PartitionRing) {
	if subring == nil {
		return
	}

	r.cache.set(subringCacheKey{identifier: identifier, shardSize: size}, subring)
}

func (r *partitionRingShuffleShardCache) getSubring(identifier string, size int) *PartitionRing {
	cached, ok := r.cache.get(subringCacheKey{identifier: identifier, shardSize: size})
	if !ok {
		return nil
	}

	return cached
}

func (r *partitionRingShuffleShardCache) setSubringWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time, subring *PartitionRing) {
	if subring == nil {
		return
	}

	var (
		lookbackWindowStart                   = now.Add(-lookbackPeriod).Unix()
		validForLookbackWindowsStartingBefore = int64(math.MaxInt64)
	)

	for _, partition := range subring.desc.Partitions {
		stateChangedDuringLookbackWindow := partition.StateTimestamp >= lookbackWindowStart

		if stateChangedDuringLookbackWindow && partition.StateTimestamp < validForLookbackWindowsStartingBefore {
			validForLookbackWindowsStartingBefore = partition.StateTimestamp
		}
	}

	// Only update cache if subring's lookback window starts later than the previously cached subring for this identifier,
	// if there is one. This prevents cache thrashing due to different calls competing if their lookback windows start
	// before and after the time a partition state has changed.
	key := subringCacheKey{identifier: identifier, shardSize: size, lookbackPeriod: lookbackPeriod}

	if existingEntry, haveCached := r.cache.getWithLookback(key); !haveCached || existingEntry.validForLookbackWindowsStartingAfter < lookbackWindowStart {
		r.cache.setWithLookback(key, cachedSubringWithLookback[*PartitionRing]{
			subring:                               subring,
			validForLookbackWindowsStartingAfter:  lookbackWindowStart,
			validForLookbackWindowsStartingBefore: validForLookbackWindowsStartingBefore,
		})
	}
}

func (r *partitionRingShuffleShardCache) getSubringWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) *PartitionRing {
	cached, ok := r.cache.getWithLookback(subringCacheKey{identifier: identifier, shardSize: size, lookbackPeriod: lookbackPeriod})
	if !ok {
		return nil
	}

	lookbackWindowStart := now.Add(-lookbackPeriod).Unix()
	if lookbackWindowStart < cached.validForLookbackWindowsStartingAfter || lookbackWindowStart > cached.validForLookbackWindowsStartingBefore {
		// The cached subring is not valid for the lookback window that has been requested.
		return nil
	}

	return cached.subring
}

func (r *partitionRingShuffleShardCache) lenWithLookback() int {
	return r.cache.lenWithLookback()
}

func (r *partitionRingShuffleShardCache) lenWithoutLookback() int {
	return r.cache.lenWithoutLookback()
}

// partitionRingMapCache is a map-based implementation of partitionRingShuffleShardCacheStorage.
type partitionRingMapCache struct {
	mtx                  sync.RWMutex
	cacheWithoutLookback map[subringCacheKey]*PartitionRing
	cacheWithLookback    map[subringCacheKey]cachedSubringWithLookback[*PartitionRing]
}

var _ partitionRingShuffleShardCacheStorage = (*partitionRingMapCache)(nil)

func newPartitionRingMapCache() *partitionRingMapCache {
	return &partitionRingMapCache{
		cacheWithoutLookback: map[subringCacheKey]*PartitionRing{},
		cacheWithLookback:    map[subringCacheKey]cachedSubringWithLookback[*PartitionRing]{},
	}
}

func (s *partitionRingMapCache) get(key subringCacheKey) (*PartitionRing, bool) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	cached, ok := s.cacheWithoutLookback[key]
	return cached, ok
}

func (s *partitionRingMapCache) set(key subringCacheKey, value *PartitionRing) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.cacheWithoutLookback[key] = value
}

func (s *partitionRingMapCache) getWithLookback(key subringCacheKey) (cachedSubringWithLookback[*PartitionRing], bool) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	cached, ok := s.cacheWithLookback[key]
	return cached, ok
}

func (s *partitionRingMapCache) setWithLookback(key subringCacheKey, value cachedSubringWithLookback[*PartitionRing]) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.cacheWithLookback[key] = value
}

func (s *partitionRingMapCache) lenWithLookback() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return len(s.cacheWithLookback)
}

func (s *partitionRingMapCache) lenWithoutLookback() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return len(s.cacheWithoutLookback)
}

// partitionRingLRUStorage is an LRU-based implementation of partitionRingShuffleShardCacheStorage.
type partitionRingLRUStorage struct {
	cacheWithoutLookback *lru.Cache[subringCacheKey, *PartitionRing]
	cacheWithLookback    *lru.Cache[subringCacheKey, cachedSubringWithLookback[*PartitionRing]]
}

var _ partitionRingShuffleShardCacheStorage = (*partitionRingLRUStorage)(nil)

func newPartitionRingLRUStorage(size int) (*partitionRingLRUStorage, error) {
	cacheWithoutLookback, err := lru.New[subringCacheKey, *PartitionRing](size)
	if err != nil {
		return nil, fmt.Errorf("failed to create without lookback cache: %w", err)
	}
	cacheWithLookback, err := lru.New[subringCacheKey, cachedSubringWithLookback[*PartitionRing]](size)
	if err != nil {
		return nil, fmt.Errorf("failed to create with lookback cache: %w", err)
	}
	return &partitionRingLRUStorage{
		cacheWithoutLookback: cacheWithoutLookback,
		cacheWithLookback:    cacheWithLookback,
	}, nil
}

func (s *partitionRingLRUStorage) get(key subringCacheKey) (*PartitionRing, bool) {
	return s.cacheWithoutLookback.Get(key)
}

func (s *partitionRingLRUStorage) set(key subringCacheKey, value *PartitionRing) {
	s.cacheWithoutLookback.Add(key, value)
}

func (s *partitionRingLRUStorage) getWithLookback(key subringCacheKey) (cachedSubringWithLookback[*PartitionRing], bool) {
	return s.cacheWithLookback.Get(key)
}

func (s *partitionRingLRUStorage) setWithLookback(key subringCacheKey, value cachedSubringWithLookback[*PartitionRing]) {
	s.cacheWithLookback.Add(key, value)
}

func (s *partitionRingLRUStorage) lenWithLookback() int {
	return s.cacheWithLookback.Len()
}

func (s *partitionRingLRUStorage) lenWithoutLookback() int {
	return s.cacheWithoutLookback.Len()
}
