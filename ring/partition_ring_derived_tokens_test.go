package ring

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
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

// legacyPartitionTokens returns the tokens AddPartition used to store in the ring, before they
// became derivable from the token scheme.
func legacyPartitionTokens(id int32) []uint32 {
	return NewSpreadMinimizingTokenGeneratorForInstanceAndZoneID("", int(id), 0, false).GenerateTokens(optimalTokensPerInstance, nil)
}

// newLegacyPartitionRingDesc returns a ring holding the tokens of each partition, the way a binary
// predating the token scheme wrote it.
func newLegacyPartitionRingDesc(partitionsCount int32, now time.Time) *PartitionRingDesc {
	desc := NewPartitionRingDesc()

	for id := int32(0); id < partitionsCount; id++ {
		desc.Partitions[id] = PartitionDesc{
			Id:             id,
			Tokens:         legacyPartitionTokens(id),
			State:          PartitionActive,
			StateTimestamp: now.Unix(),
		}
		desc.AddOrUpdateOwner(fmt.Sprintf("ingester-zone-a-%d", id), OwnerActive, id, now)
	}

	return desc
}

// newDerivedPartitionRingDesc returns the same ring as newLegacyPartitionRingDesc, with the tokens
// left to the reader to derive.
func newDerivedPartitionRingDesc(partitionsCount int32, now time.Time) *PartitionRingDesc {
	desc := NewPartitionRingDesc()

	for id := int32(0); id < partitionsCount; id++ {
		desc.AddPartition(id, PartitionActive, now)
		desc.AddOrUpdateOwner(fmt.Sprintf("ingester-zone-a-%d", id), OwnerActive, id, now)
	}

	return desc
}

func withMaxDerivedPartitionID(t *testing.T, maxID int) {
	prev := maxDerivedPartitionID.Load()
	require.NoError(t, SetMaxDerivedPartitionID(maxID))
	t.Cleanup(func() { maxDerivedPartitionID.Store(prev) })
}

func withTokenScheme(t *testing.T, name string, scheme partitionTokenScheme) {
	_, exists := partitionTokenSchemes[name]
	require.False(t, exists, "scheme %s is already registered", name)

	partitionTokenSchemes[name] = scheme
	t.Cleanup(func() { delete(partitionTokenSchemes, name) })
}

func TestPartitionRingDesc_AddPartition_ShouldDeclareTokenSchemeAndStoreNoTokens(t *testing.T) {
	desc := NewPartitionRingDesc()
	desc.AddPartition(3, PartitionActive, time.Now())

	assert.Empty(t, desc.Partitions[3].Tokens)
	assert.Equal(t, TokenSchemeSpreadMinimizing512, desc.Partitions[3].TokenScheme)
}

func TestPartitionRingCodec_ShouldRoundTripPartitionsWithoutTokens(t *testing.T) {
	const partitionsCount = 128

	now := time.Now()
	codec := GetPartitionRingCodec()

	derived := newDerivedPartitionRingDesc(partitionsCount, now)
	legacy := newLegacyPartitionRingDesc(partitionsCount, now)

	derivedEncoded, err := codec.Encode(derived)
	require.NoError(t, err)

	legacyEncoded, err := codec.Encode(legacy)
	require.NoError(t, err)

	assert.Less(t, len(derivedEncoded), len(legacyEncoded)/3)

	decoded, err := codec.Decode(derivedEncoded)
	require.NoError(t, err)
	assert.Equal(t, derived, decoded)
}

func TestPartitionRingCodec_Decode_ShouldValidateWithoutMaterializingTokens(t *testing.T) {
	codec := GetPartitionRingCodec()

	encoded, err := codec.Encode(newDerivedPartitionRingDesc(3, time.Now()))
	require.NoError(t, err)

	decoded, err := codec.Decode(encoded)
	require.NoError(t, err)

	for id, partition := range decoded.(*PartitionRingDesc).Partitions {
		assert.Empty(t, partition.Tokens, "partition %d", id)
		assert.Equal(t, TokenSchemeSpreadMinimizing512, partition.TokenScheme, "partition %d", id)
	}
}

func TestPartitionRingCodec_Decode_ShouldAcceptPartitionsCarryingTheirOwnTokens(t *testing.T) {
	codec := GetPartitionRingCodec()

	legacy := newLegacyPartitionRingDesc(3, time.Now())
	encoded, err := codec.Encode(legacy)
	require.NoError(t, err)

	decoded, err := codec.Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, legacy, decoded)
}

func TestPartitionRingCodec_Decode_ShouldPanicOnInadmissiblePartitions(t *testing.T) {
	withMaxDerivedPartitionID(t, 128)

	tests := map[string]struct {
		partition     PartitionDesc
		expectedPanic string
	}{
		"unknown token scheme": {
			partition:     PartitionDesc{Id: 1, TokenScheme: "smt-v2-512", State: PartitionActive},
			expectedPanic: `partition 1 declares unknown token scheme "smt-v2-512": every gossip member must know a token scheme before any writer emits it`,
		},
		"no tokens and no token scheme": {
			partition:     PartitionDesc{Id: 1, State: PartitionActive},
			expectedPanic: "partition 1 carries neither tokens nor a token scheme: the value was likely relayed by a binary predating the token scheme, which drops it",
		},
		"partition ID above the maximum derivable one": {
			partition:     PartitionDesc{Id: 129, TokenScheme: TokenSchemeSpreadMinimizing512, State: PartitionActive},
			expectedPanic: `partition 129 declares token scheme "smt-v1-512" but its ID is outside the derivable partition ID range [0, 128]: raise -partition-ring.max-derived-partition-id`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			codec := GetPartitionRingCodec()

			desc := NewPartitionRingDesc()
			desc.Partitions[testData.partition.Id] = testData.partition

			encoded, err := codec.Encode(desc)
			require.NoError(t, err)

			require.PanicsWithError(t, testData.expectedPanic, func() {
				_, _ = codec.Decode(encoded)
			})
		})
	}
}

func TestNewPartitionRing_ShouldMaterializeDerivedTokens(t *testing.T) {
	const partitionsCount = 8

	now := time.Now()
	derivedDesc := newDerivedPartitionRingDesc(partitionsCount, now)

	derivedRing, err := NewPartitionRing(*derivedDesc)
	require.NoError(t, err)

	legacyRing, err := NewPartitionRing(*newLegacyPartitionRingDesc(partitionsCount, now))
	require.NoError(t, err)

	for id, partition := range legacyRing.desc.Partitions {
		assert.Equal(t, partition.Tokens, derivedRing.desc.Partitions[id].Tokens, "partition %d", id)
	}

	assert.Equal(t, legacyRing.ringTokens, derivedRing.ringTokens)
	assert.Equal(t, legacyRing.partitionByToken, derivedRing.partitionByToken)

	for key := uint32(0); key < 1000; key++ {
		expected, err := legacyRing.ActivePartitionForKey(key * 4_000_000)
		require.NoError(t, err)

		actual, err := derivedRing.ActivePartitionForKey(key * 4_000_000)
		require.NoError(t, err)

		assert.Equal(t, expected, actual, "key %d", key)
	}

	// The caller's desc must not be modified: PartitionRing documents its desc as an immutable
	// snapshot, and the partitions map is shared with the caller.
	for id, partition := range derivedDesc.Partitions {
		assert.Empty(t, partition.Tokens, "partition %d", id)
	}
}

func TestNewPartitionRing_ShouldMaterializeTheNumberOfTokensDeclaredByTheScheme(t *testing.T) {
	const scheme = "smt-v1-128"

	withTokenScheme(t, scheme, partitionTokenScheme{generator: spreadMinimizingGeneratorV1, tokensCount: 128})

	desc := NewPartitionRingDesc()
	desc.Partitions[1] = PartitionDesc{Id: 1, TokenScheme: scheme, State: PartitionActive}

	ring, err := NewPartitionRing(*desc)
	require.NoError(t, err)

	tokens := ring.desc.Partitions[1].Tokens
	assert.Equal(t, legacyPartitionTokens(1)[:128], tokens)
	assert.Equal(t, 128, cap(tokens), "the memoized tokens must not be appendable to by consumers")
}

func TestNewPartitionRing_ShouldNotDeriveTokensOfPartitionsCarryingThem(t *testing.T) {
	desc := NewPartitionRingDesc()
	desc.Partitions[1] = PartitionDesc{Id: 1, Tokens: []uint32{1, 2, 3}, State: PartitionActive}

	ring, err := NewPartitionRing(*desc)
	require.NoError(t, err)

	assert.Equal(t, []uint32{1, 2, 3}, ring.desc.Partitions[1].Tokens)
}

func TestNewPartitionRing_ShouldMaterializeIdempotentlyOnShuffleShard(t *testing.T) {
	now := time.Now()
	ring, err := NewPartitionRing(*newDerivedPartitionRingDesc(8, now))
	require.NoError(t, err)

	subring, err := ring.ShuffleShard("tenant-1", 2)
	require.NoError(t, err)
	require.Equal(t, 2, subring.PartitionsCount())

	for id, partition := range subring.desc.Partitions {
		assert.Equal(t, legacyPartitionTokens(id), partition.Tokens, "partition %d", id)
	}
}

func TestNewPartitionRing_ShouldPanicOnPartitionsWhichCannotBeMaterialized(t *testing.T) {
	desc := NewPartitionRingDesc()
	desc.Partitions[1] = PartitionDesc{Id: 1, TokenScheme: "smt-v2-512", State: PartitionActive}

	require.PanicsWithError(t, `partition 1 declares unknown token scheme "smt-v2-512": every gossip member must know a token scheme before any writer emits it`, func() {
		_, _ = NewPartitionRing(*desc)
	})
}

func TestRegisterPartitionTokensFlags_ShouldControlTheMaxDerivedPartitionID(t *testing.T) {
	prev := maxDerivedPartitionID.Load()
	maxDerivedPartitionID.Store(DefaultMaxDerivedPartitionID)
	t.Cleanup(func() { maxDerivedPartitionID.Store(prev) })

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	RegisterPartitionTokensFlags(fs)

	assert.Equal(t, strconv.Itoa(DefaultMaxDerivedPartitionID), fs.Lookup("partition-ring.max-derived-partition-id").DefValue)

	require.NoError(t, fs.Parse([]string{"-partition-ring.max-derived-partition-id=64"}))
	assert.Equal(t, int32(64), maxDerivedPartitionID.Load())

	require.Error(t, fs.Parse([]string{"-partition-ring.max-derived-partition-id=-1"}))
}

func TestWarmDerivedPartitionTokens_ShouldDeriveEveryDerivablePartitionID(t *testing.T) {
	require.NoError(t, WarmDerivedPartitionTokens())

	maxID := maxDerivedPartitionID.Load()
	desc := NewPartitionRingDesc()
	desc.AddPartition(maxID, PartitionActive, time.Now())

	ring, err := NewPartitionRing(*desc)
	require.NoError(t, err)
	assert.Equal(t, legacyPartitionTokens(maxID), ring.desc.Partitions[maxID].Tokens)
}

func TestGenerateAllTokensUpTo_ShouldMatchTheTokensGeneratedForASingleID(t *testing.T) {
	tokens, err := GenerateAllTokensUpTo(20)
	require.NoError(t, err)
	require.Len(t, tokens, 21)

	for _, id := range []int32{0, 1, 7, 20} {
		assert.Equal(t, Tokens(legacyPartitionTokens(id)), tokens[id], "partition %d", id)
	}
}

// BenchmarkGenerateAllTokensUpTo measures a whole derivation pass, which is what a process pays
// once when it warms up the memo, for the whole derivable partition ID range. The generator computes
// the tokens of every lower partition to place the ones of maxID anyway, so this is also the cost of
// deriving a single partition ID.
func BenchmarkGenerateAllTokensUpTo(b *testing.B) {
	for _, maxID := range []int32{128, 1024, DefaultMaxDerivedPartitionID, 8192} {
		b.Run(fmt.Sprintf("max partition ID %d", maxID), func(b *testing.B) {
			b.ReportAllocs()

			for n := 0; n < b.N; n++ {
				if _, err := GenerateAllTokensUpTo(maxID); err != nil {
					b.Fatal("unexpected error:", err)
				}
			}
		})
	}
}

func TestPartitionRingWatcher_ShouldExportDerivedTokensMetrics(t *testing.T) {
	const ringKey = "ring"

	ctx := context.Background()
	logger := log.NewNopLogger()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	reg := prometheus.NewPedanticRegistry()
	watcher := NewPartitionRingWatcher("test", ringKey, store, logger, reg)

	require.NoError(t, services.StartAndAwaitRunning(ctx, watcher))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, watcher))
	})

	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(1, PartitionActive, time.Now())
		desc.Partitions[2] = PartitionDesc{Id: 2, Tokens: legacyPartitionTokens(2), State: PartitionActive}
		return desc, true, nil
	}))

	require.Eventually(t, func() bool {
		return watcher.PartitionRing().PartitionsCount() == 2
	}, time.Second, 10*time.Millisecond)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP partition_ring_partitions_with_derived_tokens Number of partitions in the partitions ring whose tokens have been derived from their token scheme.
		# TYPE partition_ring_partitions_with_derived_tokens gauge
		partition_ring_partitions_with_derived_tokens{name="test"} 1
	`), "partition_ring_partitions_with_derived_tokens"))
}
