package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

func TestPartitionRing_ActivePartitionForKey(t *testing.T) {
	tokensForPartition := func(partitionID int32) Tokens {
		tg := NewSpreadMinimizingTokenGeneratorForInstanceAndZoneID("", int(partitionID), 0, false)
		return tg.GenerateTokens(optimalTokensPerInstance, nil)
	}

	type addPartition struct {
		state PartitionState
	}
	tests := map[string]struct {
		partitions []addPartition
		key        uint32

		expPartitionID int32
		expErr         error
	}{
		"no partitions": {
			key:    1,
			expErr: ErrNoActivePartitionFound,
		},
		"one partition": {
			partitions: []addPartition{
				{state: PartitionActive},
			},
			key:            1,
			expPartitionID: 0,
		},
		"one inactive partition": {
			partitions: []addPartition{
				{state: PartitionInactive},
			},
			key:    1,
			expErr: ErrNoActivePartitionFound,
		},
		"one deleted partition": {
			partitions: []addPartition{
				{state: PartitionDeleted},
			},
			key:    1,
			expErr: ErrNoActivePartitionFound,
		},
		"multiple active partitions": {
			partitions: []addPartition{
				{state: PartitionActive},
				{state: PartitionActive},
				{state: PartitionActive},
			},
			key:            tokensForPartition(2)[123] - 10,
			expPartitionID: 2,
		},
		"multiple active partitions, one inactive partition": {
			partitions: []addPartition{
				{state: PartitionActive},
				{state: PartitionActive},
				{state: PartitionInactive},
			},
			key:            tokensForPartition(2)[123] - 10,
			expPartitionID: 1, // partition 2 is skipped, because it's inactive
		},
		"multiple active partitions, two inactive partitions": {
			partitions: []addPartition{
				{state: PartitionActive},
				{state: PartitionInactive},
				{state: PartitionInactive},
			},
			key:            tokensForPartition(2)[123] - 10,
			expPartitionID: 0, // partition 1 and 2 are skipped, because they are inactive
		},
		"multiple active partitions, token exactly matches one partition's token": {
			partitions: []addPartition{
				{state: PartitionActive},
				{state: PartitionActive},
				{state: PartitionActive},
			},
			key:            tokensForPartition(1)[123],
			expPartitionID: 0, // since we want token > key, the token is assigned to another partition (and because tokens are equally spread).
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			desc := NewPartitionRingDesc()
			for pIdx, p := range testCase.partitions {
				desc.AddPartition(int32(pIdx), p.state, time.Now())
			}
			const heartbeatTimeout = -1 // heartbeat timeout doesn't matter in this test
			pr := NewPartitionRing(*desc, heartbeatTimeout)

			partitionID, partition, err := pr.ActivePartitionForKey(testCase.key)

			assert.ErrorIs(t, err, testCase.expErr)
			if testCase.expErr == nil {
				assert.Equal(t, testCase.expPartitionID, partitionID)
				assert.EqualValues(t, tokensForPartition(testCase.expPartitionID), partition.GetTokens())
			}
		})
	}
}
