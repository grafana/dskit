package ring

import (
	"fmt"
	"math"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

type partitionRingShuffleShardCache interface {
	getSubring(identifier string, size int) *PartitionRing
	setSubring(identifier string, size int, subring *PartitionRing)
	getSubringWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) *PartitionRing
	setSubringWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time, subring *PartitionRing)
	lenWithLookback() int
	lenWithoutLookback() int
}

type partitionRingShuffleShardMapCache struct {
	mtx                  sync.RWMutex
	cacheWithoutLookback map[subringCacheKey]*PartitionRing
	cacheWithLookback    map[subringCacheKey]cachedSubringWithLookback[*PartitionRing]
}

func newPartitionRingShuffleShardMapCache() *partitionRingShuffleShardMapCache {
	return &partitionRingShuffleShardMapCache{
		cacheWithoutLookback: map[subringCacheKey]*PartitionRing{},
		cacheWithLookback:    map[subringCacheKey]cachedSubringWithLookback[*PartitionRing]{},
	}
}

func (r *partitionRingShuffleShardMapCache) setSubring(identifier string, size int, subring *PartitionRing) {
	if subring == nil {
		return
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.cacheWithoutLookback[subringCacheKey{identifier: identifier, shardSize: size}] = subring
}

func (r *partitionRingShuffleShardMapCache) getSubring(identifier string, size int) *PartitionRing {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	cached := r.cacheWithoutLookback[subringCacheKey{identifier: identifier, shardSize: size}]
	if cached == nil {
		return nil
	}

	return cached
}

func (r *partitionRingShuffleShardMapCache) setSubringWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time, subring *PartitionRing) {
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

	r.mtx.Lock()
	defer r.mtx.Unlock()

	// Only update cache if subring's lookback window starts later than the previously cached subring for this identifier,
	// if there is one. This prevents cache thrashing due to different calls competing if their lookback windows start
	// before and after the time a partition state has changed.
	key := subringCacheKey{identifier: identifier, shardSize: size, lookbackPeriod: lookbackPeriod}

	if existingEntry, haveCached := r.cacheWithLookback[key]; !haveCached || existingEntry.validForLookbackWindowsStartingAfter < lookbackWindowStart {
		r.cacheWithLookback[key] = cachedSubringWithLookback[*PartitionRing]{
			subring:                               subring,
			validForLookbackWindowsStartingAfter:  lookbackWindowStart,
			validForLookbackWindowsStartingBefore: validForLookbackWindowsStartingBefore,
		}
	}
}

func (r *partitionRingShuffleShardMapCache) getSubringWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) *PartitionRing {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	cached, ok := r.cacheWithLookback[subringCacheKey{identifier: identifier, shardSize: size, lookbackPeriod: lookbackPeriod}]
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

func (r *partitionRingShuffleShardMapCache) lenWithLookback() int {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	return len(r.cacheWithLookback)
}

func (r *partitionRingShuffleShardMapCache) lenWithoutLookback() int {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	return len(r.cacheWithoutLookback)
}

// partitionRingShuffleShardLRUCache is a bounded LRU-based implementation of partitionRingShuffleShardCache.
type partitionRingShuffleShardLRUCache struct {
	cacheWithoutLookback *lru.Cache[subringCacheKey, *PartitionRing]
	cacheWithLookback    *lru.Cache[subringCacheKey, cachedSubringWithLookback[*PartitionRing]]
}

var _ partitionRingShuffleShardCache = (*partitionRingShuffleShardLRUCache)(nil)

func newPartitionRingShuffleShardLRUCache(size int) (*partitionRingShuffleShardLRUCache, error) {
	cacheWithoutLookback, err := lru.New[subringCacheKey, *PartitionRing](size)
	if err != nil {
		return nil, fmt.Errorf("failed to create without lookback cache: %w", err)
	}
	cacheWithLookback, err := lru.New[subringCacheKey, cachedSubringWithLookback[*PartitionRing]](size)
	if err != nil {
		return nil, fmt.Errorf("failed to create with lookback cache: %w", err)
	}
	return &partitionRingShuffleShardLRUCache{
		cacheWithoutLookback: cacheWithoutLookback,
		cacheWithLookback:    cacheWithLookback,
	}, nil
}

func (r *partitionRingShuffleShardLRUCache) setSubring(identifier string, size int, subring *PartitionRing) {
	if subring == nil {
		return
	}

	r.cacheWithoutLookback.Add(subringCacheKey{identifier: identifier, shardSize: size}, subring)
}

func (r *partitionRingShuffleShardLRUCache) getSubring(identifier string, size int) *PartitionRing {
	cached, ok := r.cacheWithoutLookback.Get(subringCacheKey{identifier: identifier, shardSize: size})
	if !ok {
		return nil
	}

	return cached
}

func (r *partitionRingShuffleShardLRUCache) setSubringWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time, subring *PartitionRing) {
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

	if existingEntry, haveCached := r.cacheWithLookback.Get(key); !haveCached || existingEntry.validForLookbackWindowsStartingAfter < lookbackWindowStart {
		r.cacheWithLookback.Add(key, cachedSubringWithLookback[*PartitionRing]{
			subring:                               subring,
			validForLookbackWindowsStartingAfter:  lookbackWindowStart,
			validForLookbackWindowsStartingBefore: validForLookbackWindowsStartingBefore,
		})
	}
}

func (r *partitionRingShuffleShardLRUCache) getSubringWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) *PartitionRing {
	cached, ok := r.cacheWithLookback.Get(subringCacheKey{identifier: identifier, shardSize: size, lookbackPeriod: lookbackPeriod})
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

func (r *partitionRingShuffleShardLRUCache) lenWithLookback() int {
	return r.cacheWithLookback.Len()
}

func (r *partitionRingShuffleShardLRUCache) lenWithoutLookback() int {
	return r.cacheWithoutLookback.Len()
}
