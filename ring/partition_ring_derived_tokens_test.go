package ring

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/services"
)

func storedPartitionTokens(id int32) []uint32 {
	return NewSpreadMinimizingTokenGeneratorForInstanceAndZoneID("", int(id), 0, false).GenerateTokens(optimalTokensPerInstance, nil)
}

func newStoredTokensPartitionRingDesc(partitionsCount int32, now time.Time) *PartitionRingDesc {
	desc := NewPartitionRingDesc()

	for id := int32(0); id < partitionsCount; id++ {
		desc.AddPartition(id, PartitionActive, now)
		desc.AddOrUpdateOwner(fmt.Sprintf("ingester-zone-a-%d", id), OwnerActive, id, now)
	}

	return desc
}

func newTokenlessPartitionRingDesc(partitionsCount int32, now time.Time) *PartitionRingDesc {
	desc := NewPartitionRingDesc()

	for id := int32(0); id < partitionsCount; id++ {
		desc.AddPartitionWithoutTokens(id, PartitionActive, now)
		desc.AddOrUpdateOwner(fmt.Sprintf("ingester-zone-a-%d", id), OwnerActive, id, now)
	}

	return desc
}

func newDerivedTokensRing(t *testing.T, desc PartitionRingDesc, maxPartitionID int32) (*PartitionRing, error) {
	t.Helper()

	generator, err := NewPartitionTokenGenerator(maxPartitionID, log.NewNopLogger(), nil)
	require.NoError(t, err)

	return newPartitionRing(desc, DefaultPartitionRingOptions(), generator)
}

func TestPartitionRingDesc_AddPartitionWithoutTokens_ShouldDifferFromAddPartitionOnlyByTheTokens(t *testing.T) {
	now := time.Now()

	tokenless := NewPartitionRingDesc()
	tokenless.AddPartitionWithoutTokens(3, PartitionActive, now)

	stored := NewPartitionRingDesc()
	stored.AddPartition(3, PartitionActive, now)

	assert.Empty(t, tokenless.Partitions[3].Tokens)

	withTokens := tokenless.Partitions[3]
	withTokens.Tokens = storedPartitionTokens(3)
	assert.Equal(t, stored.Partitions[3], withTokens)
}

func TestPartitionRingCodec_ShouldRoundTripPartitionsWithoutTokens(t *testing.T) {
	const partitionsCount = 128

	now := time.Now()
	codec := GetPartitionRingCodec()

	tokenless := newTokenlessPartitionRingDesc(partitionsCount, now)
	stored := newStoredTokensPartitionRingDesc(partitionsCount, now)

	tokenlessEncoded, err := codec.Encode(tokenless)
	require.NoError(t, err)

	storedEncoded, err := codec.Encode(stored)
	require.NoError(t, err)

	assert.Less(t, len(tokenlessEncoded), len(storedEncoded)/3)

	// The codec does not change the tokens, so a member that only relays the ring never derives them.
	decoded, err := codec.Decode(tokenlessEncoded)
	require.NoError(t, err)
	assert.Equal(t, tokenless, decoded)
}

func TestPartitionInstanceLifecycler_ShouldCreateThePartitionWithoutTokensWhenConfiguredTo(t *testing.T) {
	tests := map[string]struct {
		omitTokens     bool
		expectedTokens []uint32
	}{
		"tokens omitted": {omitTokens: true, expectedTokens: nil},
		"tokens stored":  {omitTokens: false, expectedTokens: storedPartitionTokens(1)},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			const ringKey = "ring"

			ctx := context.Background()
			logger := log.NewNopLogger()

			store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), logger, nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			cfg := createTestPartitionInstanceLifecyclerConfig(1, "instance-1")
			cfg.OmitTokens = testData.omitTokens

			lifecycler := NewPartitionInstanceLifecycler(cfg, "test", ringKey, store, logger, nil)
			require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
			})

			partition := getPartitionRingFromStore(t, store, ringKey).Partitions[1]
			assert.Equal(t, testData.expectedTokens, partition.Tokens)
		})
	}
}

func TestNewPartitionRing_ShouldDeriveTheTokensOfPartitionsCarryingNone(t *testing.T) {
	const partitionsCount = 32

	now := time.Now()
	tokenlessDesc := newTokenlessPartitionRingDesc(partitionsCount, now)

	derivedRing, err := newDerivedTokensRing(t, *tokenlessDesc, partitionsCount)
	require.NoError(t, err)

	storedRing, err := NewPartitionRing(*newStoredTokensPartitionRingDesc(partitionsCount, now))
	require.NoError(t, err)

	assert.Equal(t, storedRing.desc.Partitions, derivedRing.desc.Partitions)
	assert.Equal(t, storedRing.ringTokens, derivedRing.ringTokens)
	assert.Equal(t, storedRing.partitionByToken, derivedRing.partitionByToken)

	for key := uint32(0); key < 1000; key++ {
		expected, err := storedRing.ActivePartitionForKey(key * 4_000_000)
		require.NoError(t, err)

		actual, err := derivedRing.ActivePartitionForKey(key * 4_000_000)
		require.NoError(t, err)

		assert.Equal(t, expected, actual, "key %d", key)
	}

	// The input desc is shared with the KV store, so it must not change.
	for id, partition := range tokenlessDesc.Partitions {
		assert.Empty(t, partition.Tokens, "partition %d", id)
	}
}

func TestNewPartitionRing_ShouldIgnoreTheTokensAPartitionCarries(t *testing.T) {
	tests := map[string][]uint32{
		"no tokens":                        {},
		"the tokens a writer stores":       storedPartitionTokens(1),
		"an arbitrary number of tokens":    {1, 2, 3},
		"a single arbitrary token":         {1},
		"more tokens than a writer stores": make([]uint32, optimalTokensPerInstance+1),
	}

	for testName, tokens := range tests {
		t.Run(testName, func(t *testing.T) {
			desc := NewPartitionRingDesc()
			desc.Partitions[1] = PartitionDesc{Id: 1, Tokens: tokens, State: PartitionActive}

			ring, err := newDerivedTokensRing(t, *desc, 1)
			require.NoError(t, err)

			assert.Equal(t, storedPartitionTokens(1), ring.desc.Partitions[1].Tokens)
		})
	}
}

func TestNewPartitionRing_ShouldNotDeriveTokensWithoutAGenerator(t *testing.T) {
	tests := map[string][]uint32{
		"no tokens":                        {},
		"an arbitrary number of tokens":    {1, 2, 3},
		"a single arbitrary token":         {1},
		"more tokens than a writer stores": make([]uint32, optimalTokensPerInstance+1),
	}

	for testName, tokens := range tests {
		t.Run(testName, func(t *testing.T) {
			desc := NewPartitionRingDesc()
			desc.Partitions[1] = PartitionDesc{Id: 1, Tokens: tokens, State: PartitionActive}

			ring, err := NewPartitionRing(*desc)
			require.NoError(t, err)

			assert.Equal(t, tokens, ring.desc.Partitions[1].Tokens)
		})
	}
}

func TestNewPartitionRing_ShouldDeriveTokensIdempotentlyOnShuffleShard(t *testing.T) {
	const partitionsCount = 16

	now := time.Now()

	derivedRing, err := newDerivedTokensRing(t, *newTokenlessPartitionRingDesc(partitionsCount, now), partitionsCount)
	require.NoError(t, err)

	storedRing, err := NewPartitionRing(*newStoredTokensPartitionRingDesc(partitionsCount, now))
	require.NoError(t, err)

	derivedSubring, err := derivedRing.ShuffleShard("tenant-1", 4)
	require.NoError(t, err)
	require.Equal(t, 4, derivedSubring.PartitionsCount())

	storedSubring, err := storedRing.ShuffleShard("tenant-1", 4)
	require.NoError(t, err)

	assert.Equal(t, storedSubring.desc.Partitions, derivedSubring.desc.Partitions)
}

func TestNewPartitionRing_ShouldReturnErrorOnPartitionsOutsideTheDerivedRange(t *testing.T) {
	tests := map[string]PartitionDesc{
		"a partition carrying no tokens":  {Id: 129, State: PartitionActive},
		"a partition carrying its tokens": {Id: 129, Tokens: storedPartitionTokens(129), State: PartitionActive},
	}

	for testName, partition := range tests {
		t.Run(testName, func(t *testing.T) {
			desc := NewPartitionRingDesc()
			desc.Partitions[partition.Id] = partition

			_, err := newDerivedTokensRing(t, *desc, 128)
			require.ErrorContains(t, err, "partition 129 is outside the derived partition ID range [0, 128]")
		})
	}
}

func TestPartitionRingDesc_MergeShouldNotReplaceTheTokensOfAPartitionItHolds(t *testing.T) {
	now := time.Now()

	stored := newStoredTokensPartitionRingDesc(3, now)
	tokenless := newTokenlessPartitionRingDesc(3, now)

	// A merge copies a new partition as it is, with or without tokens.
	merged := NewPartitionRingDesc()
	_, err := merged.mergeWithTime(tokenless.Clone(), false, now)
	require.NoError(t, err)
	assert.Equal(t, tokenless, merged)

	// A later value with tokens does not restore them. A known partition keeps the tokens it was
	// created with, so only a new ring state can remove them.
	updated := stored.Clone().(*PartitionRingDesc)
	changed, err := updated.UpdatePartitionState(1, PartitionInactive, now.Add(time.Second))
	require.NoError(t, err)
	require.True(t, changed)

	_, err = merged.mergeWithTime(updated, false, now)
	require.NoError(t, err)

	assert.Equal(t, PartitionInactive, merged.Partitions[1].State)
	assert.Empty(t, merged.Partitions[1].Tokens)
}

func TestPartitionTokenGenerator_ShouldServeThePartitionIDRangeItWasBuiltFor(t *testing.T) {
	generator, err := NewPartitionTokenGenerator(4, log.NewNopLogger(), nil)
	require.NoError(t, err)

	assert.Equal(t, int32(4), generator.MaxPartitionID())

	for _, id := range []int32{0, 1, 4} {
		tokens, err := generator.forPartition(id)
		require.NoError(t, err)
		assert.Equal(t, Tokens(storedPartitionTokens(id)), tokens, "partition %d", id)
		assert.Equal(t, optimalTokensPerInstance, cap(tokens), "the tokens must not be appendable to by consumers")
	}

	_, err = generator.forPartition(5)
	assert.ErrorContains(t, err, "outside the derived partition ID range [0, 4]")

	_, err = NewPartitionTokenGenerator(-1, log.NewNopLogger(), nil)
	assert.ErrorContains(t, err, "max partition ID -1 is negative")
}

func TestPartitionTokenGenerator_ShouldExposeTheDerivedRange(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	_, err := NewPartitionTokenGenerator(64, log.NewNopLogger(), reg)
	require.NoError(t, err)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_derived_tokens_max_partition_id Highest partition ID the derived partition tokens have been generated for.
		# TYPE partition_ring_derived_tokens_max_partition_id gauge
		partition_ring_derived_tokens_max_partition_id 64
	`), "partition_ring_derived_tokens_max_partition_id"))
}

func TestPartitionRingWatcher_ShouldDeriveTokensWhenGivenAPartitionTokenGenerator(t *testing.T) {
	const (
		ringKey         = "ring"
		partitionsCount = 4
	)

	ctx := context.Background()
	logger := log.NewNopLogger()
	now := time.Now()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	require.NoError(t, store.CAS(ctx, ringKey, func(interface{}) (interface{}, bool, error) {
		return newTokenlessPartitionRingDesc(partitionsCount, now), true, nil
	}))

	generator, err := NewPartitionTokenGenerator(partitionsCount, log.NewNopLogger(), nil)
	require.NoError(t, err)

	watcher := NewPartitionRingWatcher("test", ringKey, store, logger, nil).WithPartitionTokenGenerator(generator)
	require.NoError(t, services.StartAndAwaitRunning(ctx, watcher))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, watcher))
	})

	expected, err := NewPartitionRing(*newStoredTokensPartitionRingDesc(partitionsCount, now))
	require.NoError(t, err)

	assert.Equal(t, expected.desc.Partitions, watcher.PartitionRing().desc.Partitions)
}

func TestPartitionRingWatcher_ShouldNotDeriveTokensWithoutAPartitionTokenGenerator(t *testing.T) {
	const (
		ringKey         = "ring"
		partitionsCount = 4
	)

	ctx := context.Background()
	logger := log.NewNopLogger()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	require.NoError(t, store.CAS(ctx, ringKey, func(interface{}) (interface{}, bool, error) {
		return newTokenlessPartitionRingDesc(partitionsCount, time.Now()), true, nil
	}))

	watcher := NewPartitionRingWatcher("test", ringKey, store, logger, nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, watcher))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, watcher))
	})

	ring := watcher.PartitionRing()
	require.Equal(t, partitionsCount, ring.PartitionsCount())

	for id, partition := range ring.desc.Partitions {
		assert.Empty(t, partition.Tokens, "partition %d", id)
	}
}

func TestGenerateAllTokensUpTo_ShouldMatchTheTokensGeneratedForASingleID(t *testing.T) {
	tokens, err := generateAllTokensUpTo(20)
	require.NoError(t, err)
	require.Len(t, tokens, 21)

	for _, id := range []int32{0, 1, 7, 20} {
		assert.Equal(t, Tokens(storedPartitionTokens(id)), tokens[id], "partition %d", id)
	}
}

func BenchmarkGenerateAllTokensUpTo(b *testing.B) {
	for _, maxID := range []int32{128, 1024, DefaultMaxDerivedPartitionID, 8192} {
		b.Run(fmt.Sprintf("max partition ID %d", maxID), func(b *testing.B) {
			b.ReportAllocs()

			for n := 0; n < b.N; n++ {
				if _, err := generateAllTokensUpTo(maxID); err != nil {
					b.Fatal("unexpected error:", err)
				}
			}
		})
	}
}
