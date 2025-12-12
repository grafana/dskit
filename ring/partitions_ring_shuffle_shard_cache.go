package ring

import (
	"fmt"
	"math"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

type partitionRingShuffleShardCache struct {
	mtx                  sync.RWMutex
	cacheWithoutLookback *lru.Cache[subringCacheKey, *PartitionRing]
	cacheWithLookback    *lru.Cache[subringCacheKey, cachedSubringWithLookback[*PartitionRing]]
}

func newPartitionRingShuffleShardCache() (*partitionRingShuffleShardCache, error) {
	cacheWithoutLookback, err := lru.New[subringCacheKey, *PartitionRing](128)
	if err != nil {
		return nil, fmt.Errorf("failed to create without lookback cache: %w", err)
	}
	cacheWithLookback, err := lru.New[subringCacheKey, cachedSubringWithLookback[*PartitionRing]](128)
	if err != nil {
		return nil, fmt.Errorf("failed to create with lookback cache: %w", err)
	}
	return &partitionRingShuffleShardCache{
		cacheWithoutLookback: cacheWithoutLookback,
		cacheWithLookback:    cacheWithLookback,
	}, nil
}

func (r *partitionRingShuffleShardCache) setSubring(identifier string, size int, subring *PartitionRing) {
	if subring == nil {
		return
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.cacheWithoutLookback.Add(subringCacheKey{identifier: identifier, shardSize: size}, subring)
}

func (r *partitionRingShuffleShardCache) getSubring(identifier string, size int) *PartitionRing {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	cached, ok := r.cacheWithoutLookback.Get(subringCacheKey{identifier: identifier, shardSize: size})
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

	r.mtx.Lock()
	defer r.mtx.Unlock()

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

func (r *partitionRingShuffleShardCache) getSubringWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) *PartitionRing {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

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
