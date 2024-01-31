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
			pr := NewPartitionRing(*desc)

			partitionID, partition, err := pr.ActivePartitionForKey(testCase.key)

			assert.ErrorIs(t, err, testCase.expErr)
			if testCase.expErr == nil {
				assert.Equal(t, testCase.expPartitionID, partitionID)
				assert.EqualValues(t, tokensForPartition(testCase.expPartitionID), partition.GetTokens())
			}
		})
	}
}

func TestPartitionRing_ShuffleShardWithLookback(t *testing.T) {
	type eventType int

	const (
		add eventType = iota
		update
		remove
		test

		lookbackPeriod = time.Hour
		userID         = "user-1"
	)

	now := time.Now()
	outsideLookback := now.Add(-2 * lookbackPeriod)
	withinLookback := now.Add(-lookbackPeriod / 2)

	type event struct {
		what          eventType
		partitionID   int32
		partitionDesc PartitionDesc
		shardSize     int
		expected      []int32
	}

	tests := map[string]struct {
		partitionTokens map[int32][]uint32
		timeline        []event
	}{
		"shard size > number of partitions": {
			partitionTokens: map[int32][]uint32{
				0: {userToken(userID, "", 0) + 1},
				1: {userToken(userID, "", 1) + 1},
				2: {userToken(userID, "", 2) + 1},
			},
			timeline: []event{
				{what: add, partitionID: 0, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: add, partitionID: 1, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: add, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: test, shardSize: 4, expected: []int32{0, 1, 2}},
			},
		},
		"shard size < number of partitions": {
			partitionTokens: map[int32][]uint32{
				0: {userToken(userID, "", 0) + 1},
				1: {userToken(userID, "", 1) + 1},
				2: {userToken(userID, "", 2) + 1},
			},
			timeline: []event{
				{what: add, partitionID: 0, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: add, partitionID: 1, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: add, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: test, shardSize: 1, expected: []int32{0}},
			},
		},
		"new partitions added": {
			partitionTokens: map[int32][]uint32{
				0: {userToken(userID, "", 0) + 1},
				1: {userToken(userID, "", 2) + 1},
				2: {userToken(userID, "", 1) + 1},
			},
			timeline: []event{
				{what: add, partitionID: 0, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: add, partitionID: 1, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{0, 1}},
				{what: add, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, withinLookback)},
				{what: test, shardSize: 2, expected: []int32{0, 1, 2}},
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{0, 2}},
			},
		},
		"shard size = 2, partition is marked as inactive then marked as active again": {
			partitionTokens: map[int32][]uint32{
				0: {userToken(userID, "", 1) + 1},
				1: {userToken(userID, "", 2) + 1},
				2: {userToken(userID, "", 0) + 1},
			},
			timeline: []event{
				{what: add, partitionID: 0, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: add, partitionID: 1, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: add, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 0}},
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionInactive, withinLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}},
				// Partition 2 still inactive, but now falls outside the lookback period
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionInactive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}},
				// Partition 2 becomes active again
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, withinLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}},
				// Partition 2 still active, but now falls outside the lookback period, there's no need to look back to partition 1
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 0}},
			},
		},
		"shard size = 2, partitions marked as inactive, deleted, and re-added": {
			partitionTokens: map[int32][]uint32{
				0: {userToken(userID, "", 1) + 1},
				1: {userToken(userID, "", 2) + 1},
				2: {userToken(userID, "", 0) + 1},
			},
			timeline: []event{
				{what: add, partitionID: 0, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: add, partitionID: 1, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: add, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 0}},
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionInactive, withinLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}},
				// Partition 2 still inactive, but now falls outside the lookback period
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionInactive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}},
				// Partition 2 now gone
				{what: remove, partitionID: 2},
				{what: test, shardSize: 2, expected: []int32{1, 0}},
				// Partition 2 becomes active again
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, withinLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}},
				// Partition 2 still active, but now falls outside the lookback period, there's no need to look back to partition 1
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 0}},
			},
		},
		"increase shard size": {
			partitionTokens: map[int32][]uint32{
				0: {userToken(userID, "", 0) + 1},
				1: {userToken(userID, "", 1) + 1},
				2: {userToken(userID, "", 2) + 1},
			},
			timeline: []event{
				{what: add, partitionID: 0, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: add, partitionID: 1, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: add, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, outsideLookback)},
				{what: test, shardSize: 1, expected: []int32{0}},
				{what: test, shardSize: 2, expected: []int32{0, 1}},
				{what: test, shardSize: 3, expected: []int32{0, 1, 2}},
				{what: test, shardSize: 4, expected: []int32{0, 1, 2}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ringDesc := NewPartitionRingDesc()

			// Replay the events on the timeline.
			for ix, event := range testData.timeline {
				switch event.what {
				case add, update:
					var ok bool
					event.partitionDesc.Tokens, ok = testData.partitionTokens[event.partitionID]
					require.True(t, ok, "no tokens for partition %d", event.partitionID)
					ringDesc.Partitions[event.partitionID] = event.partitionDesc
				case remove:
					delete(ringDesc.Partitions, event.partitionID)
				case test:
					// Create a new ring for every test so that it can create its own
					ring := NewPartitionRing(*ringDesc)
					shuffledRing, err := ring.ShuffleShardWithLookback(userID, event.shardSize, lookbackPeriod, now)
					assert.NoError(t, err, "step %d", ix)
					assert.ElementsMatch(t, event.expected, maps.Keys(shuffledRing.desc.Partitions), "step %d", ix)
				}
			}
		})
	}
}

func generatePartitionWithInfo(state PartitionState, stateTS time.Time) PartitionDesc {
	return PartitionDesc{
		State:          state,
		StateTimestamp: stateTS.Unix(),
	}
}
