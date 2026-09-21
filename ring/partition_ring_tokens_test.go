package ring

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type partitionTokenGeneratorFunc func(int32) (Tokens, error)

func (f partitionTokenGeneratorFunc) TokensFor(id int32) (Tokens, error) {
	return f(id)
}

func TestPartitionRing_DerivedTokensRouteLikeStoredTokens(t *testing.T) {
	// Partitions 0-5 are active and evenly spread. Partitions 6 (inactive) and 7 own
	// small ranges after token 0, so shards have an inactive partition to skip.
	stored := NewPartitionRingDesc()
	for id := int32(0); id < 6; id++ {
		stored.Partitions[id] = PartitionDesc{Id: id, State: PartitionActive, StateTimestamp: 1, Tokens: []uint32{uint32(id) << 28, uint32(id+8) << 28}}
	}
	stored.Partitions[7] = PartitionDesc{Id: 7, State: PartitionActive, Tokens: []uint32{3, 4}}
	stored.Partitions[6] = PartitionDesc{Id: 6, State: PartitionInactive, Tokens: []uint32{1, 2}}
	want, err := NewPartitionRing(*stored)
	require.NoError(t, err)

	// Each case derives some partitions. The generator returns the reference tokens,
	// so every ring must route like the reference ring.
	for _, mode := range []string{"all derived", "mixed", "all stored"} {
		t.Run(mode, func(t *testing.T) {
			desc := stored.Clone().(*PartitionRingDesc)
			for id, partition := range desc.Partitions {
				if mode == "all derived" || (mode == "mixed" && id%2 == 0) {
					// Derived partitions must ignore any stale stored tokens.
					partition.Tokens = []uint32{uint32(id)}
					partition.TokenScheme = PartitionTokensSmt512
					desc.Partitions[id] = partition
				}
			}
			before := desc.Clone()
			var calls []int32
			opts := DefaultPartitionRingOptions()
			opts.TokenGenerator = partitionTokenGeneratorFunc(func(id int32) (Tokens, error) {
				calls = append(calls, id)
				return stored.Partitions[id].Tokens, nil
			})
			got, err := NewPartitionRingWithOptions(*desc, opts)
			require.NoError(t, err)

			// The generator is called once for each derived partition and never for stored ones.
			var derivedIDs []int32
			for id, partition := range desc.Partitions {
				if partition.TokenScheme == PartitionTokensSmt512 {
					derivedIDs = append(derivedIDs, id)
				}
			}
			assert.ElementsMatch(t, derivedIDs, calls)
			assert.Equal(t, before, desc, "resolving must not modify the descriptor")

			assertEquivalentPartitionRouting(t, want, got)

			// Shuffle shards, nested shards and lookback shards resolve derived tokens the same way.
			for _, tenant := range []string{"tenant-a", "tenant-b", "tenant-c"} {
				wantShard, err := want.ShuffleShard(tenant, 3)
				require.NoError(t, err)
				gotShard, err := got.ShuffleShard(tenant, 3)
				require.NoError(t, err)
				assertEquivalentPartitionRouting(t, wantShard, gotShard)
				assert.NotNil(t, gotShard.opts.TokenGenerator)
				// Shards keep the partition's scheme, not its resolved tokens.
				for id := range gotShard.desc.Partitions {
					assert.Equal(t, desc.Partitions[id], gotShard.desc.Partitions[id])
				}

				wantNested, err := wantShard.ShuffleShard(tenant, 1)
				require.NoError(t, err)
				gotNested, err := gotShard.ShuffleShard(tenant, 1)
				require.NoError(t, err)
				assertEquivalentPartitionRouting(t, wantNested, gotNested)

				wantLookback, err := want.ShuffleShardWithLookback(tenant, 3, time.Hour, time.Unix(3600, 0))
				require.NoError(t, err)
				gotLookback, err := got.ShuffleShardWithLookback(tenant, 3, time.Hour, time.Unix(3600, 0))
				require.NoError(t, err)
				assertEquivalentPartitionRouting(t, wantLookback, gotLookback)
			}
			assert.Equal(t, before, desc, "building shards must not modify the descriptor")
		})
	}
}

// assertEquivalentPartitionRouting checks that two rings have the same partitions, ownership,
// routing and token ranges.
func assertEquivalentPartitionRouting(t *testing.T, want, got *PartitionRing) {
	t.Helper()
	assert.Equal(t, want.PartitionIDs(), got.PartitionIDs())
	assert.Equal(t, want.countTokens(), got.countTokens())
	keys := []uint32{0, 1, math.MaxUint32}
	for _, token := range want.ringTokens {
		keys = append(keys, token-1, token, token+1)
	}
	for _, key := range keys {
		wantID, wantErr := want.ActivePartitionForKey(key)
		gotID, gotErr := got.ActivePartitionForKey(key)
		require.Equal(t, wantErr, gotErr)
		require.Equal(t, wantID, gotID, "key %d", key)
	}
	for _, id := range want.PartitionIDs() {
		wantRanges, err := want.GetTokenRangesForPartition(id)
		require.NoError(t, err)
		gotRanges, err := got.GetTokenRangesForPartition(id)
		require.NoError(t, err)
		assert.Equal(t, wantRanges, gotRanges)
	}
}

func TestPartitionRing_WithoutDerivedPartitionsSkipsGenerator(t *testing.T) {
	for _, tc := range []struct {
		name  string
		desc  PartitionRingDesc
		owned map[int32]int64
	}{
		{name: "empty ring", desc: *NewPartitionRingDesc(), owned: map[int32]int64{}},
		// A stored partition without tokens keeps zero ownership instead of being derived.
		{name: "empty stored tokens", desc: PartitionRingDesc{Partitions: map[int32]PartitionDesc{42: {State: PartitionActive}}}, owned: map[int32]int64{42: 0}},
	} {
		for _, withGenerator := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/with generator=%t", tc.name, withGenerator), func(t *testing.T) {
				opts := DefaultPartitionRingOptions()
				if withGenerator {
					opts.TokenGenerator = partitionTokenGeneratorFunc(func(int32) (Tokens, error) {
						t.Error("rings without derived partitions must not call the generator")
						return nil, nil
					})
				}
				r, err := NewPartitionRingWithOptions(tc.desc, opts)
				require.NoError(t, err)
				assert.Equal(t, tc.owned, r.countTokens())
			})
		}
	}
}

func TestPartitionRing_FailsWhenTokensCannotBeResolved(t *testing.T) {
	for _, tc := range []struct {
		name      string
		scheme    PartitionTokenScheme
		generator PartitionTokenGenerator
	}{
		{name: "no generator", scheme: PartitionTokensSmt512},
		{name: "generator error", scheme: PartitionTokensSmt512, generator: partitionTokenGeneratorFunc(func(int32) (Tokens, error) {
			return nil, errors.New("generator unavailable")
		})},
		// A working generator shows that an unknown scheme fails before any generation.
		{name: "unknown scheme", scheme: 99, generator: partitionTokenGeneratorFunc(func(int32) (Tokens, error) {
			return Tokens{42}, nil
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			desc := PartitionRingDesc{Partitions: map[int32]PartitionDesc{42: {State: PartitionActive, Tokens: []uint32{42}, TokenScheme: tc.scheme}}}
			opts := DefaultPartitionRingOptions()
			opts.TokenGenerator = tc.generator
			_, err := NewPartitionRingWithOptions(desc, opts)
			require.Error(t, err)
		})
	}
}

func TestGeneratePartitionTokens_MatchesAddPartition(t *testing.T) {
	for _, maxID := range []int32{0, 1, 15, 32} {
		t.Run(fmt.Sprint(maxID), func(t *testing.T) {
			tokens, err := generatePartitionTokens(maxID)
			require.NoError(t, err)
			require.Len(t, tokens, int(maxID)+1)
			desc := NewPartitionRingDesc()
			for id := int32(0); id <= maxID; id++ {
				desc.AddPartition(id, PartitionActive, time.Unix(1, 0))
				assert.Equal(t, Tokens(desc.Partitions[id].Tokens), tokens[id], "partition %d", id)
			}
		})
	}
	t.Run("negative ID", func(t *testing.T) {
		_, err := generatePartitionTokens(-1)
		require.Error(t, err)
	})
}

// Members relay partitions they cannot resolve, so even an unknown scheme must survive a round trip.
func TestPartitionRingDesc_TokenSchemeSurvivesEncodeAndClone(t *testing.T) {
	for _, scheme := range []PartitionTokenScheme{PartitionTokensStored, PartitionTokensSmt512, 99} {
		t.Run(fmt.Sprint(scheme), func(t *testing.T) {
			desc := NewPartitionRingDesc()
			desc.Partitions[1] = PartitionDesc{Id: 1, Tokens: []uint32{10}, TokenScheme: scheme, State: PartitionActive, StateTimestamp: 1}
			encoded, err := GetPartitionRingCodec().Encode(desc)
			require.NoError(t, err)
			decoded, err := GetPartitionRingCodec().Decode(encoded)
			require.NoError(t, err)
			assert.Equal(t, desc, decoded)
			assert.Equal(t, desc, desc.Clone())
		})
	}
}

func BenchmarkPartitionRing_TokenSchemeEncodedSize(b *testing.B) {
	tokens, err := generatePartitionTokens(4524)
	require.NoError(b, err)
	for _, count := range []int{100, 512, 2048, 4525} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			stored, derived := partitionRingTokenSchemeDescriptors(tokens[:count])
			codec := GetPartitionRingCodec()
			var storedBytes, derivedBytes []byte
			for b.Loop() {
				var err error
				storedBytes, err = codec.Encode(stored)
				require.NoError(b, err)
				derivedBytes, err = codec.Encode(derived)
				require.NoError(b, err)
			}
			b.ReportMetric(float64(len(storedBytes)), "stored-bytes/ring")
			b.ReportMetric(float64(len(storedBytes))/float64(count), "stored-bytes/partition")
			b.ReportMetric(float64(len(derivedBytes)), "derived-bytes/ring")
			b.ReportMetric(float64(len(derivedBytes))/float64(count), "derived-bytes/partition")
		})
	}
}

func partitionRingTokenSchemeDescriptors(tokens []Tokens) (*PartitionRingDesc, *PartitionRingDesc) {
	stored, derived := NewPartitionRingDesc(), NewPartitionRingDesc()
	for id, partitionTokens := range tokens {
		stored.Partitions[int32(id)] = PartitionDesc{Id: int32(id), Tokens: partitionTokens, State: PartitionActive, StateTimestamp: 1700000000}
		derived.AddPartitionWithDerivedTokens(int32(id), PartitionActive, time.Unix(1700000000, 0))
	}
	return stored, derived
}
