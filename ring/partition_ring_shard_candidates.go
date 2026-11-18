package ring

import (
	"slices"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

const (
	partitionShardCandidateLimit = 128
	partitionShardCandidateTTL   = 30 * time.Second
	// Limit the extra index and descriptor storage in proportion to the parent ring.
	partitionShardCandidateBudgetMultiplier = 4
)

func recordPartitionShardFill(recent **simplelru.LRU[subringCacheKey, struct{}], key subringCacheKey) {
	if *recent == nil {
		// NewLRU only rejects nonpositive sizes.
		*recent, _ = simplelru.NewLRU[subringCacheKey, struct{}](partitionShardCandidateLimit, nil)
	}
	(*recent).Add(key, struct{}{})
}

type partitionShardCandidateKey struct {
	subringCacheKey
	withLookback bool
}

type partitionRingShardCandidates struct {
	mu      sync.Mutex
	entries map[partitionShardCandidateKey]cachedSubringWithLookback[*PartitionRing]
	timer   *time.Timer
}

func (c *partitionRingShuffleShardCache) candidatesFor(next *PartitionRing) *partitionRingShardCandidates {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	var entries map[partitionShardCandidateKey]cachedSubringWithLookback[*PartitionRing]
	tokenBudget := partitionShardCandidateBudgetMultiplier * cap(next.ringTokens)
	descBudget := partitionShardCandidateBudgetMultiplier * (len(next.desc.Partitions) + len(next.desc.Owners))
	add := func(key partitionShardCandidateKey, candidate cachedSubringWithLookback[*PartitionRing]) {
		if key.shardSize > next.nonPendingPartitionsCount {
			return
		}
		if key.lookbackPeriod <= 0 && key.shardSize >= next.activePartitionsCount {
			return
		}
		shard := candidate.subring
		tokens := cap(shard.ringTokens)
		descs := len(shard.desc.Partitions) + len(shard.desc.Owners)
		if tokens > tokenBudget || descs > descBudget {
			return
		}
		tokenBudget -= tokens
		descBudget -= descs

		// Callers can populate a shard's nested caches while this candidate is alive.
		// Retain only the immutable data needed to validate and rebuild the shard.
		detached := *shard
		detached.shuffleShardCache = nil
		detached.prevShardCandidates = nil
		detached.allActive = nil
		candidate.subring = &detached
		if entries == nil {
			entries = make(map[partitionShardCandidateKey]cachedSubringWithLookback[*PartitionRing])
		}
		entries[key] = candidate
	}
	if c.recentWithoutLookback != nil {
		for _, key := range slices.Backward(c.recentWithoutLookback.Keys()) {
			if len(entries) == partitionShardCandidateLimit {
				break
			}
			if shard, ok := c.cacheWithoutLookback.peek(key); ok {
				add(partitionShardCandidateKey{subringCacheKey: key}, cachedSubringWithLookback[*PartitionRing]{subring: shard})
			}
		}
	}
	if len(entries) < partitionShardCandidateLimit && c.recentWithLookback != nil {
		for _, key := range slices.Backward(c.recentWithLookback.Keys()) {
			if len(entries) == partitionShardCandidateLimit {
				break
			}
			if candidate, ok := c.cacheWithLookback.peek(key); ok {
				add(partitionShardCandidateKey{subringCacheKey: key, withLookback: true}, candidate)
			}
		}
	}
	if len(entries) == 0 {
		return nil
	}
	candidates := &partitionRingShardCandidates{entries: entries}
	candidates.mu.Lock()
	candidates.timer = time.AfterFunc(partitionShardCandidateTTL, candidates.close)
	candidates.mu.Unlock()
	return candidates
}

// lookup reports whether the key is tracked even if the candidate is outside the requested window.
func (c *partitionRingShardCandidates) lookup(key partitionShardCandidateKey, now time.Time) (*PartitionRing, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if ok && key.withLookback && !entry.validForLookbackWindowStartingAt(now.Add(-key.lookbackPeriod).Unix()) {
		return nil, true
	}
	return entry.subring, ok
}

func (c *partitionRingShardCandidates) discard(key partitionShardCandidateKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
	if len(c.entries) == 0 {
		c.closeLocked()
	}
}

func (c *partitionRingShardCandidates) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
}

func (c *partitionRingShardCandidates) closeLocked() {
	c.entries = nil
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
}
