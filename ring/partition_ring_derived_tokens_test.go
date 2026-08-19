package ring

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/services"
)

func legacyPartitionTokens(id int32) []uint32 {
	return NewSpreadMinimizingTokenGeneratorForInstanceAndZoneID("", int(id), 0, false).GenerateTokens(optimalTokensPerInstance, nil)
}

func newLegacyPartitionRingDesc(partitionsCount int32, now time.Time) *PartitionRingDesc {
	desc := NewPartitionRingDesc()

	for id := int32(0); id < partitionsCount; id++ {
		desc.AddPartition(id, PartitionActive, now)
		desc.AddOrUpdateOwner(fmt.Sprintf("ingester-zone-a-%d", id), OwnerActive, id, now)
	}

	return desc
}

func newDerivedPartitionRingDesc(t *testing.T, partitionsCount int32, now time.Time) *PartitionRingDesc {
	t.Helper()

	desc := NewPartitionRingDesc()

	for id := int32(0); id < partitionsCount; id++ {
		require.NoError(t, desc.AddPartitionWithTokenScheme(id, PartitionActive, TokenSchemeSpreadMinimizing512, now))
		desc.AddOrUpdateOwner(fmt.Sprintf("ingester-zone-a-%d", id), OwnerActive, id, now)
	}

	return desc
}

func newDerivedTokensRing(t *testing.T, desc PartitionRingDesc, maxPartitionID int32) (*PartitionRing, error) {
	t.Helper()

	generator, err := NewPartitionTokenGenerator(maxPartitionID)
	require.NoError(t, err)

	return newPartitionRing(desc, DefaultPartitionRingOptions(), generator)
}

func TestPartitionRingDesc_AddPartition_ShouldStoreTokens(t *testing.T) {
	desc := NewPartitionRingDesc()
	desc.AddPartition(3, PartitionActive, time.Now())

	assert.Equal(t, legacyPartitionTokens(3), desc.Partitions[3].Tokens)
	assert.Empty(t, desc.Partitions[3].TokenScheme)
}

func TestPartitionRingDesc_AddPartitionWithTokenScheme_ShouldDeclareSchemeAndStoreNoTokens(t *testing.T) {
	desc := NewPartitionRingDesc()
	require.NoError(t, desc.AddPartitionWithTokenScheme(3, PartitionActive, TokenSchemeSpreadMinimizing512, time.Now()))

	assert.Empty(t, desc.Partitions[3].Tokens)
	assert.Equal(t, TokenSchemeSpreadMinimizing512, desc.Partitions[3].TokenScheme)
}

func TestPartitionRingDesc_AddPartitionWithTokenScheme_ShouldStoreTokensWhenSchemeIsEmpty(t *testing.T) {
	desc := NewPartitionRingDesc()
	require.NoError(t, desc.AddPartitionWithTokenScheme(3, PartitionActive, "", time.Now()))

	assert.Equal(t, legacyPartitionTokens(3), desc.Partitions[3].Tokens)
	assert.Empty(t, desc.Partitions[3].TokenScheme)
}

func TestPartitionRingDesc_AddPartitionWithTokenScheme_ShouldRejectUnknownScheme(t *testing.T) {
	desc := NewPartitionRingDesc()

	require.ErrorContains(t, desc.AddPartitionWithTokenScheme(3, PartitionActive, "smt-v2-512", time.Now()), `unknown token scheme "smt-v2-512"`)
	assert.Empty(t, desc.Partitions)
}

func TestPartitionInstanceLifecycler_ShouldCreatePartitionsDeclaringTheConfiguredTokenScheme(t *testing.T) {
	const ringKey = "ring"

	ctx := context.Background()
	logger := log.NewNopLogger()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := createTestPartitionInstanceLifecyclerConfig(1, "instance-1")
	cfg.TokenScheme = TokenSchemeSpreadMinimizing512

	lifecycler := NewPartitionInstanceLifecycler(cfg, "test", ringKey, store, logger, nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
	})

	partition := getPartitionRingFromStore(t, store, ringKey).Partitions[1]
	assert.Equal(t, TokenSchemeSpreadMinimizing512, partition.TokenScheme)
	assert.Empty(t, partition.Tokens)
}

func TestPartitionInstanceLifecycler_ShouldFailToStartOnUnknownTokenScheme(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := createTestPartitionInstanceLifecyclerConfig(1, "instance-1")
	cfg.TokenScheme = "smt-v2-512"

	lifecycler := NewPartitionInstanceLifecycler(cfg, "test", "ring", store, logger, nil)
	require.ErrorContains(t, services.StartAndAwaitRunning(ctx, lifecycler), `unknown token scheme "smt-v2-512"`)
}

func TestPartitionRingCodec_ShouldRoundTripPartitionsWithoutTokens(t *testing.T) {
	const partitionsCount = 128

	now := time.Now()
	codec := GetPartitionRingCodec()

	derived := newDerivedPartitionRingDesc(t, partitionsCount, now)
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

	encoded, err := codec.Encode(newDerivedPartitionRingDesc(t, 3, time.Now()))
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

func TestPartitionRingCodec_Decode_ShouldRejectInadmissiblePartitions(t *testing.T) {
	tests := map[string]struct {
		partition   PartitionDesc
		expectedErr string
	}{
		"unknown token scheme": {
			partition:   PartitionDesc{Id: 1, TokenScheme: "smt-v2-512", State: PartitionActive},
			expectedErr: `partition 1 declares unknown token scheme "smt-v2-512"`,
		},
		"tokens and token scheme": {
			partition:   PartitionDesc{Id: 1, Tokens: []uint32{1}, TokenScheme: TokenSchemeSpreadMinimizing512, State: PartitionActive},
			expectedErr: `partition 1 carries tokens and declares token scheme "smt-v1-512"`,
		},
		"no tokens and no token scheme": {
			partition:   PartitionDesc{Id: 1, State: PartitionActive},
			expectedErr: "partition 1 carries neither tokens nor a token scheme: the value was likely relayed by a binary predating the token scheme, which drops it",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			codec := GetPartitionRingCodec()

			desc := NewPartitionRingDesc()
			desc.Partitions[testData.partition.Id] = testData.partition

			encoded, err := codec.Encode(desc)
			require.NoError(t, err)

			decoded, err := codec.Decode(encoded)
			require.ErrorContains(t, err, testData.expectedErr)
			assert.Nil(t, decoded)
		})
	}
}

func TestNewPartitionRing_ShouldMaterializeDerivedTokens(t *testing.T) {
	const partitionsCount = 8

	now := time.Now()
	derivedDesc := newDerivedPartitionRingDesc(t, partitionsCount, now)

	derivedRing, err := newDerivedTokensRing(t, *derivedDesc, partitionsCount)
	require.NoError(t, err)

	legacyRing, err := NewPartitionRing(*newLegacyPartitionRingDesc(partitionsCount, now))
	require.NoError(t, err)

	for id, partition := range legacyRing.desc.Partitions {
		assert.Equal(t, partition.Tokens, derivedRing.desc.Partitions[id].Tokens, "partition %d", id)
		assert.Empty(t, derivedRing.desc.Partitions[id].TokenScheme, "partition %d", id)
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

	for id, partition := range derivedDesc.Partitions {
		assert.Empty(t, partition.Tokens, "partition %d", id)
	}
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
	ring, err := newDerivedTokensRing(t, *newDerivedPartitionRingDesc(t, 8, now), 8)
	require.NoError(t, err)

	subring, err := ring.ShuffleShard("tenant-1", 2)
	require.NoError(t, err)
	require.Equal(t, 2, subring.PartitionsCount())

	for id, partition := range subring.desc.Partitions {
		assert.Equal(t, legacyPartitionTokens(id), partition.Tokens, "partition %d", id)
	}
}

func TestNewPartitionRing_ShouldReturnErrorOnPartitionsWhichCannotBeMaterialized(t *testing.T) {
	tests := map[string]struct {
		partition      PartitionDesc
		maxPartitionID int32
		expectedErr    string
	}{
		"unknown token scheme": {
			partition:      PartitionDesc{Id: 1, TokenScheme: "smt-v2-512", State: PartitionActive},
			maxPartitionID: 1,
			expectedErr:    `partition 1 declares unknown token scheme "smt-v2-512"`,
		},
		"tokens and token scheme": {
			partition:      PartitionDesc{Id: 1, Tokens: []uint32{1}, TokenScheme: TokenSchemeSpreadMinimizing512, State: PartitionActive},
			maxPartitionID: 1,
			expectedErr:    `partition 1 carries tokens and declares token scheme "smt-v1-512"`,
		},
		"partition ID above the served range": {
			partition:      PartitionDesc{Id: 129, TokenScheme: TokenSchemeSpreadMinimizing512, State: PartitionActive},
			maxPartitionID: 128,
			expectedErr:    `partition 129 declares token scheme "smt-v1-512" but its ID is outside the served partition ID range [0, 128]`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			desc := NewPartitionRingDesc()
			desc.Partitions[testData.partition.Id] = testData.partition

			_, err := newDerivedTokensRing(t, *desc, testData.maxPartitionID)
			require.ErrorContains(t, err, testData.expectedErr)
		})
	}
}

func TestNewPartitionRing_ShouldReturnErrorWithoutATokenGenerator(t *testing.T) {
	desc := newDerivedPartitionRingDesc(t, 1, time.Now())

	_, err := NewPartitionRing(*desc)
	require.ErrorContains(t, err, `partition 0 declares token scheme "smt-v1-512" but the ring is built without a partition token generator`)
}

func TestPartitionTokenGenerator_ShouldServeThePartitionIDRangeItWasBuiltFor(t *testing.T) {
	generator, err := NewPartitionTokenGenerator(4)
	require.NoError(t, err)

	for _, id := range []int32{0, 1, 4} {
		tokens, err := generator.forPartition(id, TokenSchemeSpreadMinimizing512)
		require.NoError(t, err)
		assert.Equal(t, Tokens(legacyPartitionTokens(id)), tokens, "partition %d", id)
		assert.Equal(t, optimalTokensPerInstance, cap(tokens), "the tokens must not be appendable to by consumers")
	}

	_, err = generator.forPartition(5, TokenSchemeSpreadMinimizing512)
	assert.ErrorContains(t, err, "outside the served partition ID range [0, 4]")

	_, err = generator.forPartition(1, "smt-v2-512")
	assert.ErrorContains(t, err, `unknown token scheme "smt-v2-512"`)

	_, err = NewPartitionTokenGenerator(-1)
	assert.ErrorContains(t, err, "max partition ID -1 is negative")
}

func TestPartitionTokenGenerator_ShouldFailRatherThanResolveToNoTokens(t *testing.T) {
	generator, err := NewPartitionTokenGenerator(1)
	require.NoError(t, err)

	// Unreachable through the public API, since the served range is derived in full. A partition
	// resolved to no tokens would own nothing and silently stop receiving writes.
	delete(generator.tokens, 1)

	_, err = generator.forPartition(1, TokenSchemeSpreadMinimizing512)
	require.ErrorContains(t, err, "no derived tokens for partition 1")
}

func TestGenerateAllTokensUpTo_ShouldMatchTheTokensGeneratedForASingleID(t *testing.T) {
	tokens, err := generateAllTokensUpTo(20)
	require.NoError(t, err)
	require.Len(t, tokens, 21)

	for _, id := range []int32{0, 1, 7, 20} {
		assert.Equal(t, Tokens(legacyPartitionTokens(id)), tokens[id], "partition %d", id)
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
