package ring

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionRing_ShuffleShard_SubringIndexes(t *testing.T) {
	now := time.Unix(1792597591, 0)
	for _, cacheSize := range []int{0, 2} {
		for _, derived := range []bool{false, true} {
			for _, scenario := range []string{"active", "mixed", "tokenless", "no active", "empty"} {
				t.Run(fmt.Sprintf("cache=%d/derived=%t/%s", cacheSize, derived, scenario), func(t *testing.T) {
					desc := NewPartitionRingDesc()
					table := &PartitionTokenTable{tokens: make([]Tokens, 12)}
					active := map[int32]struct{}{}
					for id := int32(0); id < 12 && scenario != "empty"; id++ {
						state := PartitionActive
						switch scenario {
						case "mixed":
							state = []PartitionState{PartitionActive, PartitionInactive, PartitionPending}[id%3]
						case "no active":
							state = PartitionInactive
						}
						tokens := Tokens{uint32(id) << 28, uint32(id)<<28 + 1}
						if scenario == "tokenless" && id%4 == 0 {
							tokens = nil
						}
						table.tokens[id] = tokens
						partition := PartitionDesc{Id: id, State: state, StateTimestamp: now.Add(-2 * time.Hour).Unix(), Tokens: tokens}
						if state == PartitionInactive {
							partition.StateTimestamp = now.Add(-30 * time.Minute).Unix()
						}
						if derived {
							partition.TokenScheme = PartitionTokensSmt512
							partition.Tokens = nil
						}
						desc.Partitions[id] = partition
						desc.Owners[fmt.Sprintf("owner-%d", id)] = OwnerDesc{OwnedPartition: id}
						if state == PartitionActive && len(tokens) > 0 {
							active[id] = struct{}{}
						}
					}
					// Full shards must still filter owners whose partition is absent.
					desc.Owners["orphan"] = OwnerDesc{OwnedPartition: 99}
					before := desc.Clone()
					opts := PartitionRingOptions{ShuffleShardCacheSize: cacheSize}
					if derived {
						opts.TokenGenerator = table
					}
					parent, err := NewPartitionRingWithOptions(*desc, opts)
					require.NoError(t, err)
					for _, size := range []int{1, 4, 12, 0} {
						for _, lookback := range []time.Duration{0, time.Hour} {
							shard, err := parent.shuffleShard("tenant", size, lookback, now, nil)
							require.NoError(t, err)
							if lookback == 0 && (size == 0 || size >= len(active)) {
								ids := make(map[int32]struct{}, shard.PartitionsCount())
								for id := range shard.desc.Partitions {
									ids[id] = struct{}{}
								}
								assert.Equal(t, active, ids)
							}
							expected, err := NewPartitionRingWithOptions(shard.desc, opts)
							require.NoError(t, err)
							assertPartitionRingSubringIndexes(t, expected, shard)
							require.NotSame(t, parent.shuffleShardCache, shard.shuffleShardCache)
							assert.NotContains(t, shard.desc.Owners, "orphan")
							assert.Equal(t, opts, shard.opts)

							wantNested, err := expected.ShuffleShard("nested", 2)
							require.NoError(t, err)
							gotNested, err := shard.ShuffleShard("nested", 2)
							require.NoError(t, err)
							assert.Equal(t, wantNested.desc, gotNested.desc)
							assertPartitionRingSubringIndexes(t, wantNested, gotNested)
						}
					}
					assert.Equal(t, before, desc)
				})
			}
		}
	}
}

// TestPartitionRing_ShuffleShard_SharesAllActiveSubring checks that every shard covering all Active
// partitions is one subring per ring, whatever the tenant or size, and that a subring's own full
// shard covers only the subring, not its parent.
func TestPartitionRing_ShuffleShard_SharesAllActiveSubring(t *testing.T) {
	tokens, err := generatePartitionTokens(15)
	require.NoError(t, err)
	desc := NewPartitionRingDesc()
	var active []int32
	for id := int32(0); id < 16; id++ {
		// Pending and Inactive partitions keep the all-Active shard from being the whole ring, which
		// is when building it costs the most.
		state := PartitionActive
		switch {
		case id >= 14:
			state = PartitionInactive
		case id >= 12:
			state = PartitionPending
		default:
			active = append(active, id)
		}
		desc.Partitions[id] = PartitionDesc{Id: id, State: state, Tokens: tokens[id]}
		desc.Owners[fmt.Sprintf("owner-%d", id)] = OwnerDesc{OwnedPartition: id}
	}
	r, err := NewPartitionRingWithOptions(*desc, DefaultPartitionRingOptions())
	require.NoError(t, err)

	// Size 0 is the default (every partition); sizes at or above the Active count select the same.
	full, err := r.ShuffleShard("tenant-a", 0)
	require.NoError(t, err)
	assert.Equal(t, active, full.PartitionIDs())
	for _, req := range []struct {
		tenant string
		size   int
	}{{"tenant-b", 0}, {"tenant-c", len(active)}, {"tenant-d", 100}} {
		shard, err := r.ShuffleShard(req.tenant, req.size)
		require.NoError(t, err)
		assert.Same(t, full, shard, "tenant %s, size %d", req.tenant, req.size)
	}

	// A shard of a quarter of the ring or more is filtered from the parent's indexes, copying the
	// parent ring; its own full shard must still be built from its partitions alone.
	sub, err := r.ShuffleShard("tenant-e", 6)
	require.NoError(t, err)
	require.Len(t, sub.PartitionIDs(), 6)
	nested, err := sub.ShuffleShard("nested", 0)
	require.NoError(t, err)
	assert.Equal(t, sub.PartitionIDs(), nested.PartitionIDs())
}

// shardReuseDesc returns a ring of 40 Active partitions, each with two owners, and a Pending
// partition 40, for tests that change it the ways a ring changes after partitions exist.
func shardReuseDesc(t *testing.T) *PartitionRingDesc {
	t.Helper()
	tokens, err := generatePartitionTokens(40)
	require.NoError(t, err)
	desc := NewPartitionRingDesc()
	for id := int32(0); id <= 40; id++ {
		state := PartitionActive
		if id == 40 {
			state = PartitionPending
		}
		desc.Partitions[id] = PartitionDesc{Id: id, State: state, StateTimestamp: 10, Tokens: tokens[id]}
		for _, zone := range []string{"a", "b"} {
			desc.Owners[fmt.Sprintf("owner-%s-%d", zone, id)] = OwnerDesc{OwnedPartition: id, State: OwnerActive, UpdatedTimestamp: 10}
		}
	}
	return desc
}

// TestPartitionRing_ShuffleShard_ReusesUnchangedShards checks that after a ring update that isn't
// Pending-only, every shard equals one built by a fresh ring, and that exactly the shards the update
// didn't change are served from the previous ring rather than rebuilt.
func TestPartitionRing_ShuffleShard_ReusesUnchangedShards(t *testing.T) {
	// Sizes cover small shards, built from their own tokens, and a shard of a quarter of the ring or
	// more, filtered from the parent ring. Default-size shards get the shared subring instead.
	sizes := []int{2, 5, 12}
	// The hour of lookback starts at 25, after every partition's state timestamp (10), so a
	// partition whose state changes at 30 enters lookback and extends the shards that walk past it.
	const lookback = time.Hour
	now := time.Unix(3625, 0)
	for name, tc := range map[string]struct {
		change   func(d *PartitionRingDesc, id int32)
		activate bool
	}{
		"pending partition switched to active": {activate: true, change: func(d *PartitionRingDesc, id int32) {
			p := d.Partitions[id]
			p.State, p.StateTimestamp = PartitionActive, 30
			d.Partitions[id] = p
		}},
		"active partition switched to inactive": {change: func(d *PartitionRingDesc, id int32) {
			p := d.Partitions[id]
			p.State, p.StateTimestamp = PartitionInactive, 30
			d.Partitions[id] = p
		}},
		"owner of an active partition updated": {change: func(d *PartitionRingDesc, id int32) {
			d.Owners[fmt.Sprintf("owner-a-%d", id)] = OwnerDesc{OwnedPartition: id, State: OwnerActive, UpdatedTimestamp: 30}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			before := shardReuseDesc(t)
			after := shardReuseDesc(t)
			prev, err := NewPartitionRingWithOptions(*before, DefaultPartitionRingOptions())
			require.NoError(t, err)
			prev.inheritShards(nil)
			// Warm both paths before taking the snapshot. Twelve tenants fit within the budget.
			var oldShards, oldLookbackShards [12]*PartitionRing
			for i := range oldShards {
				tenant, size := fmt.Sprintf("tenant-%d", i), sizes[i%len(sizes)]
				oldShards[i], err = prev.ShuffleShard(tenant, size)
				require.NoError(t, err)
				oldLookbackShards[i], err = prev.ShuffleShardWithLookback(tenant, size, lookback, now)
				require.NoError(t, err)
			}
			// For changes to an active partition, pick one that a warmed shard selected.
			require.NotEmpty(t, oldShards[0].PartitionIDs())
			changed := oldShards[0].PartitionIDs()[0]
			if tc.activate {
				changed = 40
			}
			tc.change(after, changed)
			require.False(t, sameExceptPending(before, after), "the update must replace the current cache")
			next, err := NewPartitionRingWithOptions(*after, DefaultPartitionRingOptions())
			require.NoError(t, err)
			next.inheritShards(prev)
			require.NotNil(t, next.prevShardCandidates)
			t.Cleanup(next.prevShardCandidates.close)
			next.prevShardCandidates.mu.Lock()
			candidateCount := len(next.prevShardCandidates.entries)
			next.prevShardCandidates.mu.Unlock()
			require.Equal(t, 2*len(oldShards), candidateCount, "fixture must fit in the reuse budget")
			fresh, err := NewPartitionRingWithOptions(*after, DefaultPartitionRingOptions())
			require.NoError(t, err)

			reused, rebuilt := 0, 0
			check := func(label string, old, got, want *PartitionRing) {
				require.Equal(t, want.PartitionIDs(), got.PartitionIDs(), label)
				assert.Equal(t, want.desc, got.desc, label)
				assertPartitionRingSubringIndexes(t, want, got)
				require.NotEmpty(t, old.ringTokens)
				require.NotEmpty(t, got.ringTokens)
				if slices.Equal(old.PartitionIDs(), want.PartitionIDs()) && !slices.Contains(want.PartitionIDs(), changed) {
					require.Same(t, &old.ringTokens[0], &got.ringTokens[0], "%s: unchanged shard indexes rebuilt", label)
					reused++
				} else {
					require.NotSame(t, &old.ringTokens[0], &got.ringTokens[0], "%s: changed shard indexes reused", label)
					rebuilt++
				}
			}
			for i := range oldShards {
				tenant, size := fmt.Sprintf("tenant-%d", i), sizes[i%len(sizes)]
				label := fmt.Sprintf("%s, size %d", tenant, size)

				got, err := next.ShuffleShard(tenant, size)
				require.NoError(t, err)
				want, err := fresh.ShuffleShard(tenant, size)
				require.NoError(t, err)
				check(label, oldShards[i], got, want)

				got, err = next.ShuffleShardWithLookback(tenant, size, lookback, now)
				require.NoError(t, err)
				want, err = fresh.ShuffleShardWithLookback(tenant, size, lookback, now)
				require.NoError(t, err)
				check(label+", with lookback", oldLookbackShards[i], got, want)
			}
			// The fixture must exercise both outcomes, or the checks above prove nothing about one.
			require.Positive(t, reused, "no shard reused")
			require.Positive(t, rebuilt, "no shard rebuilt")
		})
	}
}

func assertPartitionRingSubringIndexes(t *testing.T, expected, actual *PartitionRing) {
	t.Helper()
	assert.Equal(t, expected.ringTokens, actual.ringTokens)
	assert.Equal(t, expected.ringPartitionIDs, actual.ringPartitionIDs)
	assert.Equal(t, expected.ringPartitionActive, actual.ringPartitionActive)
	assert.Equal(t, expected.partitionByToken, actual.partitionByToken)
	assert.Equal(t, expected.ownersByPartition, actual.ownersByPartition)
	assert.Equal(t, expected.activePartitionsCount, actual.activePartitionsCount)
	assert.Equal(t, expected.nonPendingPartitionsCount, actual.nonPendingPartitionsCount)
	assert.Equal(t, expected.maxPartitionID, actual.maxPartitionID)
}

func BenchmarkPartitionRing_ShuffleShardUncached(b *testing.B) {
	const partitions = 1500
	tokens, err := generatePartitionTokens(partitions - 1)
	require.NoError(b, err)
	for _, derived := range []bool{false, true} {
		desc := NewPartitionRingDesc()
		for id, partitionTokens := range tokens {
			partition := PartitionDesc{Id: int32(id), State: PartitionActive, Tokens: partitionTokens}
			if derived {
				partition.TokenScheme = PartitionTokensSmt512
				partition.Tokens = nil
			}
			desc.Partitions[int32(id)] = partition
			for replica := 0; replica < 3; replica++ {
				desc.Owners[fmt.Sprintf("owner-%d-%d", id, replica)] = OwnerDesc{OwnedPartition: int32(id)}
			}
		}
		opts := DefaultPartitionRingOptions()
		if derived {
			opts.TokenGenerator = &PartitionTokenTable{tokens: tokens}
		}
		r, err := NewPartitionRingWithOptions(*desc, opts)
		require.NoError(b, err)
		for _, size := range []int{50, 375, 500, partitions} {
			b.Run(fmt.Sprintf("derived=%t/size=%d", derived, size), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					// Full shards are memoized per ring; reset the memo so each iteration builds one.
					r.allActive = &partitionRingAllActive{}
					_, err := r.shuffleShard("tenant", size, 0, time.Time{}, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
