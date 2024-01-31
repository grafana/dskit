package ring

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

		expectedPartitionID int32
		expectedErr         error
	}{
		"no partitions": {
			key:         1,
			expectedErr: ErrNoActivePartitionFound,
		},
		"one partition": {
			partitions: []addPartition{
				{state: PartitionActive},
			},
			key:                 1,
			expectedPartitionID: 0,
		},
		"one inactive partition": {
			partitions: []addPartition{
				{state: PartitionInactive},
			},
			key:         1,
			expectedErr: ErrNoActivePartitionFound,
		},
		"one deleted partition": {
			partitions: []addPartition{
				{state: PartitionDeleted},
			},
			key:         1,
			expectedErr: ErrNoActivePartitionFound,
		},
		"multiple active partitions": {
			partitions: []addPartition{
				{state: PartitionActive},
				{state: PartitionActive},
				{state: PartitionActive},
			},
			key:                 tokensForPartition(2)[123] - 10,
			expectedPartitionID: 2,
		},
		"multiple active partitions, one inactive partition": {
			partitions: []addPartition{
				{state: PartitionActive},
				{state: PartitionActive},
				{state: PartitionInactive},
			},
			key:                 tokensForPartition(2)[123] - 10,
			expectedPartitionID: 1, // partition 2 is skipped, because it's inactive
		},
		"multiple active partitions, two inactive partitions": {
			partitions: []addPartition{
				{state: PartitionActive},
				{state: PartitionInactive},
				{state: PartitionInactive},
			},
			key:                 tokensForPartition(2)[123] - 10,
			expectedPartitionID: 0, // partition 1 and 2 are skipped, because they are inactive
		},
		"multiple active partitions, token exactly matches one partition's token": {
			partitions: []addPartition{
				{state: PartitionActive},
				{state: PartitionActive},
				{state: PartitionActive},
			},
			key:                 tokensForPartition(1)[123],
			expectedPartitionID: 0, // since we want token > key, the token is assigned to another partition (and because tokens are equally spread).
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			desc := NewPartitionRingDesc()
			for pIdx, p := range testCase.partitions {
				desc.AddPartition(int32(pIdx), p.state, time.Now())
			}
			pr := NewPartitionRing(*desc)

			partitionID, err := pr.ActivePartitionForKey(testCase.key)

			assert.ErrorIs(t, err, testCase.expectedErr)
			if testCase.expectedErr == nil {
				assert.Equal(t, testCase.expectedPartitionID, partitionID)
			}
		})
	}
}

func TestPartitionRing_ActivePartitionForKey_NoMemoryAllocations(t *testing.T) {
	const (
		numActivePartitions   = 100
		numInactivePartitions = 10
	)

	ring := createPartitionRingWithPartitions(numActivePartitions, numInactivePartitions)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	numAllocs := testing.AllocsPerRun(10, func() {
		_, err := ring.ActivePartitionForKey(r.Uint32())
		if err != nil {
			t.Fail()
		}
	})

	assert.Equal(t, float64(0), numAllocs)
}

func BenchmarkPartitionRing_ActivePartitionForKey(b *testing.B) {
	const (
		numActivePartitions   = 100
		numInactivePartitions = 10
	)

	ring := createPartitionRingWithPartitions(numActivePartitions, numInactivePartitions)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err := ring.ActivePartitionForKey(r.Uint32())
		if err != nil {
			b.Fail()
		}
	}
}

func createPartitionRingWithPartitions(numActive, numInactive int) *PartitionRing {
	desc := NewPartitionRingDesc()

	for i := 0; i < numActive; i++ {
		desc.AddPartition(int32(i), PartitionActive, time.Now())
	}
	for i := numActive; i < numActive+numInactive; i++ {
		desc.AddPartition(int32(i), PartitionInactive, time.Now())
	}

	return NewPartitionRing(*desc)
}
