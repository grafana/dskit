package ring

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveRingTokens(t *testing.T) {
	desc := PartitionRingDesc{
		Partitions: map[int32]PartitionDesc{
			1: {Tokens: []uint32{1, 5, 8}},
			2: {Tokens: []uint32{3, 4, 9}},
		},
	}

	ringTokens, partitionByToken, err := resolveRingTokens(desc, DefaultPartitionRingOptions())
	require.NoError(t, err)

	assert.Equal(t, Tokens{1, 3, 4, 5, 8, 9}, ringTokens)
	assert.Equal(t, map[Token]int32{1: 1, 5: 1, 8: 1, 3: 2, 4: 2, 9: 2}, partitionByToken)
}

func TestPartitionTokenTable_GeneratesConfiguredIDsOnce(t *testing.T) {
	t.Run("negative", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		_, err := newPartitionTokenTable(-1, log.NewNopLogger(), reg, func(int32) ([]Tokens, error) {
			t.Fatal("invalid count must not generate tokens")
			return nil, nil
		})
		require.Error(t, err)
		assertPartitionTokenTableMetrics(t, reg, "")
	})
	t.Run("zero", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		table, err := newPartitionTokenTable(0, log.NewNopLogger(), reg, func(int32) ([]Tokens, error) {
			t.Fatal("an empty table must not generate tokens")
			return nil, nil
		})
		require.NoError(t, err)
		_, err = table.TokensFor(0)
		require.Error(t, err)
		assertPartitionTokenTableMetrics(t, reg, "0")
	})
	t.Run("positive", func(t *testing.T) {
		var maxIDs []int32
		reg := prometheus.NewPedanticRegistry()
		table, err := newPartitionTokenTable(8, log.NewNopLogger(), reg, func(maxID int32) ([]Tokens, error) {
			maxIDs = append(maxIDs, maxID)
			tokens := make([]Tokens, maxID+1)
			for id := range tokens {
				tokens[id] = Tokens{uint32(id)}
			}
			return tokens, nil
		})
		require.NoError(t, err)
		require.Len(t, maxIDs, 1, "construction must generate once")
		assert.Equal(t, int32(7), maxIDs[0])

		// IDs just outside [0, 8) are rejected.
		for _, id := range []int32{-1, 8} {
			_, err := table.TokensFor(id)
			require.Error(t, err)
		}
		tokens, err := table.TokensFor(7)
		require.NoError(t, err)
		assert.Equal(t, Tokens{7}, tokens)
		assert.Len(t, maxIDs, 1, "lookups must not generate tokens")
		assertPartitionTokenTableMetrics(t, reg, "8")
	})
}

// assertPartitionTokenTableMetrics checks the table's partitions gauge, or that it isn't registered when expected is empty.
func assertPartitionTokenTableMetrics(t *testing.T, reg prometheus.Gatherer, expected string) {
	t.Helper()

	var text string
	if expected != "" {
		text = `
			# HELP partition_ring_max_derived_token_partitions Number of partition IDs, from 0, that can use derived tokens.
			# TYPE partition_ring_max_derived_token_partitions gauge
			partition_ring_max_derived_token_partitions ` + expected + "\n"
	}
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(text), "partition_ring_max_derived_token_partitions"))
}

func TestPartitionTokenTable_FailsWhenGenerationFails(t *testing.T) {
	_, err := newPartitionTokenTable(8, log.NewNopLogger(), nil, func(int32) ([]Tokens, error) {
		return nil, errors.New("generation failed")
	})
	require.Error(t, err)
}

func TestGeneratePartitionTokens_MatchesAddPartition(t *testing.T) {
	table, err := NewPartitionTokenTable(33, log.NewNopLogger(), nil)
	require.NoError(t, err)
	for _, maxID := range []int32{0, 1, 15, 32} {
		t.Run(fmt.Sprint(maxID), func(t *testing.T) {
			tokens, err := generatePartitionTokens(maxID)
			require.NoError(t, err)
			require.Len(t, tokens, int(maxID)+1)
			desc := NewPartitionRingDesc()
			for id := int32(0); id <= maxID; id++ {
				desc.AddPartition(id, PartitionActive, time.Unix(1, 0))
				assert.Equal(t, Tokens(desc.Partitions[id].Tokens), tokens[id], "partition %d", id)
				fromTable, err := table.TokensFor(id)
				require.NoError(t, err)
				assert.Equal(t, tokens[id], fromTable, "partition %d", id)
			}
		})
	}
	t.Run("negative ID", func(t *testing.T) {
		_, err := generatePartitionTokens(-1)
		require.Error(t, err)
	})
}

func BenchmarkGeneratePartitionTokens(b *testing.B) {
	for _, count := range []int{1024, 2048, 4096, 8192, 16384} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tokens, err := generatePartitionTokens(int32(count - 1))
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(tokens)
			}
		})
	}
}
