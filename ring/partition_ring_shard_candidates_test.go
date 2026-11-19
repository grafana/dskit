package ring

import (
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func (c *partitionRingShardCandidates) get(key partitionShardCandidateKey, now time.Time) *PartitionRing {
	shard, _ := c.lookup(key, now)
	return shard
}

func TestPartitionRingWatcher_ShardCandidateCollection(t *testing.T) {
	for _, cacheSize := range []int{0, 2 * partitionShardCandidateLimit} {
		t.Run(fmt.Sprintf("cache size %d", cacheSize), func(t *testing.T) {
			opts := PartitionRingOptions{ShuffleShardCacheSize: cacheSize}
			watcher := NewPartitionRingWatcherWithOptions("test", "test", nil, opts, log.NewNopLogger(), nil)
			before := shardReuseDesc(t)
			require.NoError(t, watcher.updatePartitionRing(before))
			prev := watcher.PartitionRing()
			for i := range partitionShardCandidateLimit + 1 {
				_, err := prev.ShuffleShard(fmt.Sprintf("tenant-%d", i), 1)
				require.NoError(t, err)
			}
			// The fill list must ignore the shared all-Active shard, even though it has cache entries.
			for i := range partitionShardCandidateLimit {
				_, err := prev.ShuffleShard(fmt.Sprintf("default-%d", i), 0)
				require.NoError(t, err)
			}
			// Both types compete for the same candidate budget, with writes first.
			_, err := prev.ShuffleShardWithLookback("lookback", 1, time.Hour, time.Unix(3625, 0))
			require.NoError(t, err)
			after := before.Clone().(*PartitionRingDesc)
			after.Owners["owner-a-7"] = OwnerDesc{OwnedPartition: 7, State: OwnerActive, UpdatedTimestamp: 30}
			require.NoError(t, watcher.updatePartitionRing(after))
			candidates := watcher.PartitionRing().prevShardCandidates
			require.NotNil(t, candidates)
			t.Cleanup(candidates.close)
			candidates.mu.Lock()
			count := len(candidates.entries)
			candidates.mu.Unlock()
			require.Equal(t, partitionShardCandidateLimit, count)
			for i := range partitionShardCandidateLimit + 1 {
				key := partitionShardCandidateKey{subringCacheKey: subringCacheKey{identifier: fmt.Sprintf("tenant-%d", i), shardSize: 1}}
				if i == 0 {
					require.Nil(t, candidates.get(key, time.Time{}), "the oldest fill exceeded the tracking budget")
				} else {
					require.NotNil(t, candidates.get(key, time.Time{}))
				}
			}
		})
	}
}

func TestPartitionRingWatcher_ShardCandidateStorageBudget(t *testing.T) {
	for _, limit := range []string{"token indexes", "descriptors"} {
		t.Run(limit, func(t *testing.T) {
			before := shardReuseDesc(t)
			after := before.Clone().(*PartitionRingDesc)
			moreOwners := after
			if limit == "descriptors" {
				moreOwners = before
			}
			// Use the same token data in both rings while making one ring owner-heavy.
			for id := range moreOwners.Partitions {
				for i := range 16 {
					moreOwners.Owners[fmt.Sprintf("extra-%d-%d", id, i)] = OwnerDesc{OwnedPartition: id, State: OwnerActive}
				}
			}
			watcher := NewPartitionRingWatcher("test", "test", nil, log.NewNopLogger(), nil)
			require.NoError(t, watcher.updatePartitionRing(before))
			prev := watcher.PartitionRing()
			const size = 18
			var shard *PartitionRing
			for i := range partitionShardCandidateLimit / 2 {
				var err error
				shard, err = prev.ShuffleShard(fmt.Sprintf("tenant-%d", i), size)
				require.NoError(t, err)
			}
			require.NoError(t, watcher.updatePartitionRing(after))
			next := watcher.PartitionRing()
			tokenLimit := partitionShardCandidateBudgetMultiplier * cap(next.ringTokens) / cap(shard.ringTokens)
			descLimit := partitionShardCandidateBudgetMultiplier * (len(next.desc.Partitions) + len(next.desc.Owners)) / (len(shard.desc.Partitions) + len(shard.desc.Owners))
			if limit == "token indexes" {
				require.Less(t, tokenLimit, descLimit, "fixture must reach the token limit first")
			} else {
				require.Less(t, descLimit, tokenLimit, "fixture must reach the descriptor limit first")
			}
			want := min(tokenLimit, descLimit)
			require.Positive(t, want, "at least one candidate must fit")
			require.NotNil(t, next.prevShardCandidates)
			t.Cleanup(next.prevShardCandidates.close)
			next.prevShardCandidates.mu.Lock()
			count := len(next.prevShardCandidates.entries)
			next.prevShardCandidates.mu.Unlock()
			require.Equal(t, want, count)
		})
	}
}

func TestPartitionRingWatcher_ShardCandidateExpiryAcrossPendingUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		watcher := NewPartitionRingWatcher("test", "test", nil, log.NewNopLogger(), nil)
		before := shardReuseDesc(t)
		require.NoError(t, watcher.updatePartitionRing(before))
		_, err := watcher.PartitionRing().ShuffleShard("tenant", 2)
		require.NoError(t, err)
		after := before.Clone().(*PartitionRingDesc)
		after.Owners["owner-a-7"] = OwnerDesc{OwnedPartition: 7, State: OwnerActive, UpdatedTimestamp: 30}
		require.NoError(t, watcher.updatePartitionRing(after))
		candidates := watcher.PartitionRing().prevShardCandidates
		key := partitionShardCandidateKey{subringCacheKey: subringCacheKey{identifier: "tenant", shardSize: 2}}
		require.NotNil(t, candidates)
		require.NotNil(t, candidates.get(key, time.Time{}))

		time.Sleep(partitionShardCandidateTTL / 2)
		pending := after.Clone().(*PartitionRingDesc)
		pending.Owners["owner-a-40"] = OwnerDesc{OwnedPartition: 40, State: OwnerActive, UpdatedTimestamp: 35}
		require.NoError(t, watcher.updatePartitionRing(pending))
		require.Same(t, candidates, watcher.PartitionRing().prevShardCandidates)
		require.NotNil(t, candidates.get(key, time.Time{}))

		time.Sleep(partitionShardCandidateTTL / 2)
		synctest.Wait()
		// Check the timer released the references without a shard request to trigger it.
		candidates.mu.Lock()
		entries, timer := candidates.entries, candidates.timer
		candidates.mu.Unlock()
		require.Nil(t, entries)
		require.Nil(t, timer)
	})
}

func TestPartitionRingWatcher_ShardCandidateSkipsEvictedEntries(t *testing.T) {
	opts := PartitionRingOptions{ShuffleShardCacheSize: 1}
	watcher := NewPartitionRingWatcherWithOptions("test", "test", nil, opts, log.NewNopLogger(), nil)
	before := shardReuseDesc(t)
	require.NoError(t, watcher.updatePartitionRing(before))
	prev := watcher.PartitionRing()
	_, err := prev.ShuffleShard("evicted", 2)
	require.NoError(t, err)
	old, err := prev.ShuffleShard("retained", 2)
	require.NoError(t, err)

	// Populate both the nested shard cache and the old shard's all-Active memo.
	_, err = old.ShuffleShard("nested", 0)
	require.NoError(t, err)
	after := before.Clone().(*PartitionRingDesc)
	for id := int32(0); id < 40; id++ {
		if _, included := old.desc.Partitions[id]; included {
			continue
		}
		after.Owners[fmt.Sprintf("owner-a-%d", id)] = OwnerDesc{OwnedPartition: id, State: OwnerActive, UpdatedTimestamp: 30}
		break
	}
	require.NoError(t, watcher.updatePartitionRing(after))
	next := watcher.PartitionRing()
	candidates := next.prevShardCandidates
	require.NotNil(t, candidates)
	t.Cleanup(candidates.close)
	evicted := partitionShardCandidateKey{subringCacheKey: subringCacheKey{identifier: "evicted", shardSize: 2}}
	key := partitionShardCandidateKey{subringCacheKey: subringCacheKey{identifier: "retained", shardSize: 2}}
	require.Nil(t, candidates.get(evicted, time.Time{}))
	detached := candidates.get(key, time.Time{})
	require.NotNil(t, detached)
	require.Nil(t, detached.shuffleShardCache)
	require.Nil(t, detached.allActive)
	require.Nil(t, detached.prevShardCandidates)
	// A second miss can read the candidate before the first miss has finished using it.
	require.Same(t, detached, candidates.get(key, time.Time{}))
	var results [16]struct {
		shard *PartitionRing
		err   error
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() {
			<-start
			results[i].shard, results[i].err = next.ShuffleShard("retained", 2)
		})
	}
	close(start)
	wg.Wait()
	for _, got := range results {
		require.NoError(t, got.err)
		require.NotEmpty(t, got.shard.ringTokens)
		require.Same(t, &old.ringTokens[0], &got.shard.ringTokens[0])
		require.NotSame(t, old.shuffleShardCache, got.shard.shuffleShardCache)
		require.NotSame(t, old.allActive, got.shard.allActive)
	}
	require.Nil(t, candidates.get(key, time.Time{}), "completed misses must retire their candidates")
	// The old snapshot still serves its original ordinary cache.
	stillCached, err := prev.ShuffleShard("retained", 2)
	require.NoError(t, err)
	require.Same(t, old, stillCached)
}

func TestPartitionRingWatcher_ShuffleShardWithLookback_CandidateWindow(t *testing.T) {
	watcher := NewPartitionRingWatcher("test", "test", nil, log.NewNopLogger(), nil)
	before := shardReuseDesc(t)
	// Only partition 7 is inside the early lookback window. Changing an owner of partition 8
	// forces a new ring without changing either the early or the late shard's owner set.
	for id, timestamp := range map[int32]int64{7: 30, 8: 10} {
		partition := before.Partitions[id]
		partition.State, partition.StateTimestamp = PartitionInactive, timestamp
		before.Partitions[id] = partition
	}
	require.NoError(t, watcher.updatePartitionRing(before))
	const lookback = time.Hour
	early := time.Unix(3625, 0)
	late := early.Add(10 * time.Second)
	old, err := watcher.PartitionRing().ShuffleShardWithLookback("early-first", 0, lookback, early)
	require.NoError(t, err)
	require.Equal(t, []int32{7}, old.InactivePartitionIDs())
	_, err = watcher.PartitionRing().ShuffleShardWithLookback("late-first", 0, lookback, early)
	require.NoError(t, err)

	after := before.Clone().(*PartitionRingDesc)
	after.Owners["owner-a-8"] = OwnerDesc{OwnedPartition: 8, State: OwnerActive, UpdatedTimestamp: 30}
	require.NoError(t, watcher.updatePartitionRing(after))
	next := watcher.PartitionRing()
	candidates := next.prevShardCandidates
	require.NotNil(t, candidates)
	t.Cleanup(candidates.close)
	key := partitionShardCandidateKey{
		subringCacheKey: subringCacheKey{identifier: "late-first", shardSize: next.nonPendingPartitionsCount, lookbackPeriod: lookback},
		withLookback:    true,
	}
	require.NotNil(t, candidates.get(key, early))
	require.Nil(t, candidates.get(key, late))

	got, err := next.ShuffleShardWithLookback("early-first", 0, lookback, early)
	require.NoError(t, err)
	require.NotEmpty(t, got.ringTokens)
	require.Same(t, &old.ringTokens[0], &got.ringTokens[0])
	for _, identifier := range []string{"early-first", "late-first"} {
		got, err := next.ShuffleShardWithLookback(identifier, 0, lookback, late)
		require.NoError(t, err)
		require.Empty(t, got.InactivePartitionIDs(), identifier)
	}
	require.Nil(t, candidates.get(key, early), "the late miss superseded the old candidate")
}
