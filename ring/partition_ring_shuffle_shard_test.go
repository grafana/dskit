package ring

import (
	"fmt"
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
							shard, err := parent.shuffleShard("tenant", size, lookback, now)
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

func TestPartitionRing_ShuffleShard_DuplicateTokens(t *testing.T) {
	for _, derived := range []bool{false, true} {
		t.Run(fmt.Sprintf("derived=%t", derived), func(t *testing.T) {
			desc := NewPartitionRingDesc()
			table := &PartitionTokenTable{tokens: make([]Tokens, 4)}
			for id := int32(0); id < 4; id++ {
				table.tokens[id] = Tokens{10}
				partition := PartitionDesc{Id: id, State: PartitionActive, Tokens: Tokens{10}}
				if derived {
					partition.TokenScheme = PartitionTokensSmt512
					partition.Tokens = nil
				}
				desc.Partitions[id] = partition
			}
			opts := DefaultPartitionRingOptions()
			if derived {
				opts.TokenGenerator = table
			}
			parent, err := NewPartitionRingWithOptions(*desc, opts)
			require.NoError(t, err)
			owner := parent.partitionByToken[10]
			expected, err := NewPartitionRingWithOptions(desc.WithPartitions(map[int32]struct{}{owner: {}}), opts)
			require.NoError(t, err)
			for _, size := range []int{1, 2, 0} {
				for _, lookback := range []time.Duration{0, time.Hour} {
					shard, err := parent.shuffleShard("tenant", size, lookback, time.Unix(1792597591, 0))
					require.NoError(t, err)
					assert.Equal(t, []int32{owner}, shard.PartitionIDs())
					assertPartitionRingSubringIndexes(t, expected, shard)
				}
			}
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
					_, err := r.shuffleShard("tenant", size, 0, time.Time{})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
