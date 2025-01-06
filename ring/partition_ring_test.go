package ring

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"sync"
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
		"multiple active partitions, one pending partition": {
			partitions: []addPartition{
				{state: PartitionActive},
				{state: PartitionActive},
				{state: PartitionPending},
			},
			key:                 tokensForPartition(2)[123] - 10,
			expectedPartitionID: 1, // partition 2 is skipped, because it's pending
		},
		"multiple active partitions, two pending partitions": {
			partitions: []addPartition{
				{state: PartitionActive},
				{state: PartitionPending},
				{state: PartitionPending},
			},
			key:                 tokensForPartition(2)[123] - 10,
			expectedPartitionID: 0, // partition 1 and 2 are skipped, because they are pending
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

	ring := createPartitionRingWithPartitions(numActivePartitions, numInactivePartitions, 0)
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

	ring := createPartitionRingWithPartitions(numActivePartitions, numInactivePartitions, 0)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err := ring.ActivePartitionForKey(r.Uint32())
		if err != nil {
			b.Fail()
		}
	}
}

func TestPartitionRing_ShuffleShard(t *testing.T) {
	t.Run("should honor the shard size", func(t *testing.T) {
		const numActivePartitions = 5

		ring := createPartitionRingWithPartitions(numActivePartitions, 0, 0)

		// Request a shard size up to the number of existing partitions.
		for shardSize := 1; shardSize <= numActivePartitions; shardSize++ {
			subring, err := ring.ShuffleShard("tenant-id", shardSize)
			require.NoError(t, err)
			assert.Equal(t, shardSize, subring.PartitionsCount())
			assert.Equal(t, subring.PartitionsCount(), ring.ShuffleShardSize(shardSize))
		}

		// Request a shard size greater than the number of existing partitions.
		subring, err := ring.ShuffleShard("tenant-id", numActivePartitions+1)
		require.NoError(t, err)
		assert.Equal(t, numActivePartitions, subring.PartitionsCount())
		assert.Equal(t, subring.PartitionsCount(), ring.ShuffleShardSize(numActivePartitions+1))
	})

	t.Run("should never return INACTIVE or InstanceState_PENDING partitions", func(t *testing.T) {
		const (
			numActivePartitions   = 5
			numInactivePartitions = 5
			numPendingPartitions  = 5
		)

		ring := createPartitionRingWithPartitions(numActivePartitions, numInactivePartitions, numPendingPartitions)

		// We test negative values of shardSize as well as sizes above current number of partition count.
		for shardSize := -5; shardSize <= ring.PartitionsCount()+5; shardSize++ {
			subring, err := ring.ShuffleShard("tenant-id", shardSize)
			require.NoError(t, err)

			for _, partition := range subring.Partitions() {
				assert.Equal(t, PartitionActive, partition.State)
			}
			assert.Equal(t, subring.PartitionsCount(), ring.ShuffleShardSize(shardSize))
		}
	})
}

// This test asserts on shard stability across multiple invocations and given the same input ring.
func TestPartitionRing_ShuffleShard_Stability(t *testing.T) {
	var (
		numTenants            = 100
		numActivePartitions   = 50
		numInactivePartitions = 10
		numPendingPartitions  = 5
		numInvocations        = 10
		shardSizes            = []int{3, 6, 9, 12, 15}
	)

	// Initialise the ring.
	ring := createPartitionRingWithPartitions(numActivePartitions, numInactivePartitions, numPendingPartitions)

	for i := 1; i <= numTenants; i++ {
		tenantID := fmt.Sprintf("%d", i)

		for _, size := range shardSizes {
			expectedSubring, err := ring.ShuffleShard(tenantID, size)
			require.NoError(t, err)

			assert.Equal(t, expectedSubring.PartitionsCount(), ring.ShuffleShardSize(size))

			// Assert that multiple invocations generate the same exact shard.
			for n := 0; n < numInvocations; n++ {
				actualSubring, err := ring.ShuffleShard(tenantID, size)
				require.NoError(t, err)
				assert.Equal(t, expectedSubring.desc, actualSubring.desc)
			}
		}
	}
}

func TestPartitionRing_ShuffleShard_Shuffling(t *testing.T) {
	var (
		numTenants          = 1000
		numActivePartitions = 90
		shardSize           = 3

		// This is the expected theoretical distribution of matching partitions
		// between different shards, given the settings above. It has been computed
		// using this spreadsheet:
		// https://docs.google.com/spreadsheets/d/1FXbiWTXi6bdERtamH-IfmpgFq1fNL4GP_KX_yJvbRi4/edit
		theoreticalMatchings = map[int]float64{
			0: 90.2239,
			1: 9.55312,
			2: 0.22217,
			3: 0.00085,
		}
	)

	// Initialise the ring.
	ring := createPartitionRingWithPartitions(numActivePartitions, 0, 0)

	// Compute the shard for each tenant.
	partitionsByTenant := map[string][]int32{}

	for i := 1; i <= numTenants; i++ {
		tenantID := fmt.Sprintf("%d", i)

		subring, err := ring.ShuffleShard(tenantID, shardSize)
		require.NoError(t, err)

		partitionsByTenant[tenantID] = subring.PartitionIDs()
	}

	// Compute the distribution of matching partitions between every combination of shards.
	// The shards comparison is not optimized, but it's fine for a test.
	distribution := map[int]int{}

	for currTenant, currPartitions := range partitionsByTenant {
		for otherTenant, otherPartitions := range partitionsByTenant {
			if currTenant == otherTenant {
				continue
			}

			numMatching := 0
			for _, c := range currPartitions {
				if slices.Contains(otherPartitions, c) {
					numMatching++
				}
			}

			distribution[numMatching]++
		}
	}

	maxCombinations := int(math.Pow(float64(numTenants), 2)) - numTenants
	for numMatching, probability := range theoreticalMatchings {
		// We allow a max deviance of 10% compared to the theoretical probability,
		// clamping it between 1% and 0.2% boundaries.
		maxDeviance := math.Min(1, math.Max(0.2, probability*0.1))

		actual := (float64(distribution[numMatching]) / float64(maxCombinations)) * 100
		assert.InDelta(t, probability, actual, maxDeviance, "numMatching: %d", numMatching)
	}
}

func TestPartitionRing_Sorting(t *testing.T) {
	desc := NewPartitionRingDesc()

	// Add in reverse order.
	for i := 9; i >= 0; i-- {
		desc.AddPartition(int32(i), PartitionState(i)%3+PartitionPending, time.Now())
	}

	pr := NewPartitionRing(*desc)

	require.Equal(t, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, pr.PartitionIDs())
	require.Equal(t, []int32{0, 3, 6, 9}, pr.PendingPartitionIDs())
	require.Equal(t, []int32{1, 4, 7}, pr.ActivePartitionIDs())
	require.Equal(t, []int32{2, 5, 8}, pr.InactivePartitionIDs())
}

func TestPartitionRing_ShuffleShard_ConsistencyOnPartitionsTopologyChange(t *testing.T) {
	type change string

	type scenario struct {
		name          string
		numPartitions int
		shardSize     int
		ringChange    change
	}

	const (
		numTenants                      = 100
		addActivePartition              = change("add-active-partition")
		switchPendingPartitionToActive  = change("switch-pending-partition-to-active")
		switchActivePartitionToInactive = change("switch-active-partition-to-inactive")
		removeActivePartition           = change("remove-active-partition")
		removeInactivePartition         = change("remove-inactive-partition")
	)

	// Generate all test scenarios.
	var scenarios []scenario
	for _, numPartitions := range []int{20, 30, 40, 50} {
		for _, shardSize := range []int{3, 6, 9, 12, 15} {
			for _, c := range []change{addActivePartition, switchPendingPartitionToActive, switchActivePartitionToInactive, removeActivePartition, removeInactivePartition} {
				scenarios = append(scenarios, scenario{
					name:          fmt.Sprintf("partitions = %d, shard size = %d, ring operation = %s", numPartitions, shardSize, c),
					numPartitions: numPartitions,
					shardSize:     shardSize,
					ringChange:    c,
				})
			}
		}
	}

	for _, s := range scenarios {
		s := s

		t.Run(s.name, func(t *testing.T) {
			t.Parallel()

			// Always include 5 inactive and pending partitions.
			ring := createPartitionRingWithPartitions(s.numPartitions, 5, 5)
			require.Equal(t, s.numPartitions, len(ring.ActivePartitionIDs()))
			require.Equal(t, 5, len(ring.InactivePartitionIDs()))
			require.Equal(t, 5, len(ring.PendingPartitionIDs()))

			// Compute the initial shard for each tenant.
			initial := map[int][]int32{}
			for id := 0; id < numTenants; id++ {
				subring, err := ring.ShuffleShard(fmt.Sprintf("%d", id), s.shardSize)
				require.NoError(t, err)
				initial[id] = subring.PartitionIDs()
			}

			// Update the ring.
			switch s.ringChange {
			case addActivePartition:
				desc := &(ring.desc)
				desc.AddPartition(int32(s.numPartitions+1), PartitionActive, time.Now())
				ring = NewPartitionRing(*desc)
			case switchPendingPartitionToActive:
				// Switch to first pending partition to active.
				desc := &(ring.desc)
				desc.UpdatePartitionState(ring.PendingPartitionIDs()[0], PartitionActive, time.Now())
				ring = NewPartitionRing(*desc)
			case switchActivePartitionToInactive:
				// Switch to first active partition to inactive.
				desc := &(ring.desc)
				desc.UpdatePartitionState(ring.ActivePartitionIDs()[0], PartitionInactive, time.Now())
				ring = NewPartitionRing(*desc)
			case removeActivePartition:
				// Remove the first active partition.
				desc := &(ring.desc)
				desc.RemovePartition(ring.ActivePartitionIDs()[0])
				ring = NewPartitionRing(*desc)
			case removeInactivePartition:
				// Remove the first inactive partition.
				desc := &(ring.desc)
				desc.RemovePartition(ring.InactivePartitionIDs()[0])
				ring = NewPartitionRing(*desc)
			}

			// Compute the updated shard for each tenant and compare it with the initial one.
			// If the "consistency" property is guaranteed, we expect no more than 1 different
			// partition in the updated shard.
			for id := 0; id < numTenants; id++ {
				subring, err := ring.ShuffleShard(fmt.Sprintf("%d", id), s.shardSize)
				require.NoError(t, err)
				updated := subring.PartitionIDs()

				added, removed := comparePartitionIDs(initial[id], updated)
				assert.LessOrEqual(t, len(added), 1)
				assert.LessOrEqual(t, len(removed), 1)
			}
		})
	}
}

func TestPartitionRing_ShuffleShard_ConsistencyOnShardSizeChanged(t *testing.T) {
	ring := createPartitionRingWithPartitions(30, 0, 0)

	// Get the replication set with shard size = 3.
	firstShard, err := ring.ShuffleShard("tenant-id", 3)
	require.NoError(t, err)
	assert.Equal(t, 3, firstShard.PartitionsCount())
	firstPartitions := firstShard.PartitionIDs()

	// Increase shard size to 6.
	secondShard, err := ring.ShuffleShard("tenant-id", 6)
	require.NoError(t, err)
	assert.Equal(t, 6, secondShard.PartitionsCount())
	secondPartitions := secondShard.PartitionIDs()

	for _, id := range firstPartitions {
		assert.True(t, slices.Contains(secondPartitions, id), "new shard is expected to include previous partition %s", id)
	}

	// Increase shard size to 9.
	thirdShard, err := ring.ShuffleShard("tenant-id", 9)
	require.NoError(t, err)
	assert.Equal(t, 9, thirdShard.PartitionsCount())
	thirdPartitions := thirdShard.PartitionIDs()

	for _, id := range secondPartitions {
		assert.True(t, slices.Contains(thirdPartitions, id), "new shard is expected to include previous partition %s", id)
	}

	// Decrease shard size to 6.
	fourthShard, err := ring.ShuffleShard("tenant-id", 6)
	require.NoError(t, err)
	assert.Equal(t, 6, fourthShard.PartitionsCount())
	fourthPartitions := fourthShard.PartitionIDs()

	// We expect to have the same exact instances we had when the shard size was 6.
	assert.Equal(t, secondPartitions, fourthPartitions)

	// Decrease shard size to 3.
	fifthShard, err := ring.ShuffleShard("tenant-id", 3)
	require.NoError(t, err)
	assert.Equal(t, 3, fifthShard.PartitionsCount())
	fifthPartitions := fifthShard.PartitionIDs()

	// We expect to have the same exact instances we had when the shard size was 3.
	assert.Equal(t, firstPartitions, fifthPartitions)
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
				// Partition 2 still inactive, but now falls outside the lookback period, there's no need to include partition 1
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionInactive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{1, 0}},
				// Partition 2 becomes active again
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, withinLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}},
				// Partition 2 still active, but now falls outside the lookback period, there's no need to include partition 1
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
				// Partition 2 switches to inactive
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionInactive, withinLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}},
				// Partition 2 still inactive, but now falls outside the lookback period, there's no need to include partition 1
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionInactive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{1, 0}},
				// Partition 2 now gone
				{what: remove, partitionID: 2},
				{what: test, shardSize: 2, expected: []int32{1, 0}},
				// Partition 2 becomes active again
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionActive, withinLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}},
				// Partition 2 still active, but now falls outside the lookback period, there's no need to include partition 1
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

func TestPartitionRing_ShuffleShardWithLookback_CorrectnessWithFuzzy(t *testing.T) {
	// The goal of this test is NOT to ensure that the minimum required number of partitions
	// are returned at any given time, BUT at least all required partitions are returned.
	var (
		numInitialPartitions = []int{9, 30, 60, 90}
		numEvents            = 100
		lookbackPeriod       = time.Hour
		delayBetweenEvents   = 5 * time.Minute // 12 events / hour
	)

	for _, numPartitions := range numInitialPartitions {
		numPartitions := numPartitions

		t.Run(fmt.Sprintf("partitions: %d", numPartitions), func(t *testing.T) {
			t.Parallel()

			// Randomise the seed but log it in case we need to reproduce the test on failure.
			seed := time.Now().UnixNano()
			rnd := rand.New(rand.NewSource(seed))
			t.Log("random generator seed:", seed)

			// Initialise the ring.
			ring := createPartitionRingWithPartitions(numPartitions, 0, 0)

			// The simulation starts with the minimum shard size. Random events can later increase it.
			shardSize := 1

			// Generate a random user id. Since tokens are deterministic, if we don't randomize the user ID
			// we'll not really get any variance between 2 test executions in terms of partitions in the user's shard.
			userID := randomString(rnd, 6)

			// Add the initial shard to the history.
			currTime := time.Now()
			subring, err := ring.shuffleShard(userID, shardSize, 0, currTime)
			require.NoError(t, err)

			history := map[time.Time][]int32{
				currTime: subring.PartitionIDs(),
			}

			// Advance the time. The simulation assumes the initial ring contains partitions added
			// outside of the lookback period.
			currTime = currTime.Add(lookbackPeriod).Add(time.Minute)

			// Simulate a progression of random events over the time and, at each iteration of the simuation,
			// make sure the subring includes all non-removed partitions picked from previous versions of the
			// ring up until the lookback period.
			nextPartitionID := ring.PartitionsCount()

			t.Log("Events:")

			for i := 1; i <= numEvents; i++ {
				currTime = currTime.Add(delayBetweenEvents)

				switch r := rnd.Intn(100); {
				case r < 35:
					// Add a pending partition.
					desc := &(ring.desc)
					desc.AddPartition(int32(nextPartitionID), PartitionPending, currTime)
					ring = NewPartitionRing(*desc)
					t.Logf("- Added partition %d in pending state", nextPartitionID)

					nextPartitionID++
				case r < 70:
					// Switch a pending partition to active.
					pendingIDs := ring.PendingPartitionIDs()
					if len(pendingIDs) > 0 {
						// Tests are reproducible because the list of partition IDs is sorted and the random
						// generator is initialised with a known seed.
						partitionIDToSwitch := pendingIDs[rnd.Intn(len(pendingIDs))]

						desc := &(ring.desc)
						desc.UpdatePartitionState(partitionIDToSwitch, PartitionActive, currTime)
						ring = NewPartitionRing(*desc)
						t.Logf("- Switched partition %d from pending to active state", partitionIDToSwitch)
					}
				case r < 80:
					// Switch an active partition to inactive.
					activeIDs := ring.ActivePartitionIDs()
					if len(activeIDs) > 0 {
						// Tests are reproducible because the list of partition IDs is sorted and the random
						// generator is initialised with a known seed.
						partitionIDToSwitch := activeIDs[rnd.Intn(len(activeIDs))]

						desc := &(ring.desc)
						desc.UpdatePartitionState(partitionIDToSwitch, PartitionInactive, currTime)
						ring = NewPartitionRing(*desc)
						t.Logf("- Switched partition %d from active to inactive", partitionIDToSwitch)
					}
				case r < 90:
					// Remove an inactive partition.
					inactiveIDs := ring.InactivePartitionIDs()
					if len(inactiveIDs) > 0 {
						// Tests are reproducible because the list of partition IDs is sorted and the random
						// generator is initialised with a known seed.
						partitionIDToRemove := inactiveIDs[rnd.Intn(len(inactiveIDs))]

						desc := &(ring.desc)
						desc.RemovePartition(partitionIDToRemove)
						ring = NewPartitionRing(*desc)
						t.Logf("- Removed inactive partition %d", partitionIDToRemove)

						// Remove the partition from the history.
						for ringTime, partitionIDs := range history {
							for idx, partitionID := range partitionIDs {
								if partitionID != partitionIDToRemove {
									continue
								}

								history[ringTime] = append(partitionIDs[:idx], partitionIDs[idx+1:]...)
								break
							}
						}
					}
				default:
					// Scale up shard size.
					shardSize++
					t.Logf("- Increased shard size to %d (num active partitions: %d)", shardSize, len(ring.ActivePartitionIDs()))
				}

				// Add the current shard to the history.
				subring, err = ring.shuffleShard(userID, shardSize, 0, time.Now())
				require.NoError(t, err)
				history[currTime] = subring.PartitionIDs()

				// Ensure the shard with lookback includes all partitions from previous states of the ring.
				subringWithLookback, err := ring.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, currTime)
				require.NoError(t, err)
				subringWithLookbackPartitionIDs := subringWithLookback.PartitionIDs()

				for ringTime, ringPartitionIDs := range history {
					if ringTime.Before(currTime.Add(-lookbackPeriod)) {
						// This entry from the history is obsolete, we can remove it.
						delete(history, ringTime)
						continue
					}

					for _, expectedPartitionID := range ringPartitionIDs {
						if !slices.Contains(subringWithLookbackPartitionIDs, expectedPartitionID) {
							t.Fatalf(
								"subring generated after event %d is expected to include partition %d from ring state at time %s but it's missing (actual partitions are: %v)",
								i, expectedPartitionID, ringTime.String(), subringWithLookbackPartitionIDs)
						}
					}
				}
			}
		})
	}
}

func TestPartitionRing_ShuffleShardWithLookback_Caching(t *testing.T) {
	var (
		tenantID      = "user-1"
		otherTenantID = "user-2"
		subringSize   = 3
		now           = time.Now()
	)

	desc := NewPartitionRingDesc()
	desc.AddPartition(1, PartitionActive, now.Add(-1*time.Hour))
	desc.AddPartition(2, PartitionActive, now.Add(-90*time.Minute))
	desc.AddPartition(3, PartitionActive, now.Add(-6*time.Hour))
	desc.AddPartition(4, PartitionActive, now.Add(-6*time.Hour))
	desc.AddPartition(5, PartitionActive, now.Add(-6*time.Hour))
	desc.AddPartition(6, PartitionActive, now.Add(-6*time.Hour))
	ring := NewPartitionRing(*desc)

	t.Run("identical requests", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(tenantID, subringSize, time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(tenantID, subringSize, time.Minute, now)
		require.NoError(t, err)

		require.Same(t, first, second)
		require.Equal(t, []int32{2, 3, 5}, first.PartitionIDs())
	})

	t.Run("different shard size", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(tenantID, subringSize, time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(tenantID, subringSize+1, time.Minute, now)
		require.NoError(t, err)

		require.NotSame(t, first, second)
		require.Equal(t, []int32{2, 3, 5}, first.PartitionIDs())
		require.Equal(t, []int32{1, 2, 3, 5}, second.PartitionIDs())
	})

	t.Run("different identifiers", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(tenantID, subringSize, time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, time.Minute, now)
		require.NoError(t, err)

		require.NotSame(t, first, second)
		require.Equal(t, []int32{2, 3, 5}, first.PartitionIDs())
		require.Equal(t, []int32{1, 4, 6}, second.PartitionIDs())
	})

	t.Run("different lookback windows", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(tenantID, subringSize, 30*time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(tenantID, subringSize, 80*time.Minute, now)
		require.NoError(t, err)

		third, err := ring.ShuffleShardWithLookback(tenantID, subringSize, 100*time.Minute, now)
		require.NoError(t, err)

		require.NotSame(t, first, second)
		require.NotSame(t, first, third)
		require.Equal(t, []int32{2, 3, 5}, first.PartitionIDs())
		require.Equal(t, []int32{2, 3, 5}, second.PartitionIDs())
		require.Equal(t, []int32{2, 3, 5, 6}, third.PartitionIDs())
	})

	t.Run("same lookback window at different times, both over a period touching the most recent partition state change", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 80*time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 80*time.Minute, now.Add(5*time.Minute))
		require.NoError(t, err)

		require.Same(t, first, second)
		require.Equal(t, []int32{1, 2, 4, 6}, first.PartitionIDs())
		require.Equal(t, []int32{1, 2, 4, 6}, second.PartitionIDs())
	})

	t.Run("same lookback window at different times, crossing the period of the most recent partition state change", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 80*time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 80*time.Minute, now.Add(30*time.Minute))
		require.NoError(t, err)

		require.NotSame(t, first, second)
		require.Equal(t, []int32{1, 2, 4, 6}, first.PartitionIDs())
		require.Equal(t, []int32{1, 4, 6}, second.PartitionIDs())
	})

	t.Run("same lookback window at different times, crossing the period of the 2 most recent partition state changes", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 100*time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 100*time.Minute, now.Add(60*time.Minute))
		require.NoError(t, err)

		require.NotSame(t, first, second)
		require.Equal(t, []int32{1, 2, 4, 5, 6}, first.PartitionIDs())
		require.Equal(t, []int32{1, 4, 6}, second.PartitionIDs())
	})

	t.Run("same lookback window at different alternate times, crossing the period of the most recent partition state change", func(t *testing.T) {
		firstEarly, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 62*time.Minute, now)
		require.NoError(t, err)

		firstLate, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 62*time.Minute, now.Add(5*time.Minute))
		require.NoError(t, err)

		require.NotSame(t, firstEarly, firstLate)
		require.Equal(t, []int32{1, 2, 4, 6}, firstEarly.PartitionIDs())
		require.Equal(t, []int32{1, 4, 6}, firstLate.PartitionIDs())

		// Run again the same requests.
		secondEarly, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 62*time.Minute, now)
		require.NoError(t, err)

		secondLate, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 62*time.Minute, now.Add(5*time.Minute))
		require.NoError(t, err)

		require.NotSame(t, secondEarly, secondLate)
		require.Equal(t, firstEarly.PartitionIDs(), secondEarly.PartitionIDs())
		require.Equal(t, firstLate.PartitionIDs(), secondLate.PartitionIDs())

		// The early cached entry should have been invalidated.
		require.NotSame(t, firstEarly, secondEarly)

		// The late cached entry is preserved because cache skips caching an entry if its timestamp is older
		// than the one already cached.
		require.Same(t, firstLate, secondLate)
	})

	t.Run("a lookback window that effectively includes all partitions", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 12*time.Hour, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 12*time.Hour, now.Add(30*time.Minute))
		require.NoError(t, err)

		require.Same(t, first, second)
		require.Equal(t, []int32{1, 2, 3, 4, 5, 6}, first.PartitionIDs())
		require.Equal(t, []int32{1, 2, 3, 4, 5, 6}, second.PartitionIDs())
	})
}

func TestPartitionRing_ShuffleShardWithLookback_CachingConcurrency(t *testing.T) {
	const (
		numWorkers            = 10
		numRequestsPerWorker  = 1000
		numActivePartitions   = 100
		numInactivePartitions = 10
		numPendingPartitions  = 10
		shardSize             = 3
		lookback              = time.Hour
	)

	ring := createPartitionRingWithPartitions(numActivePartitions, numInactivePartitions, numPendingPartitions)

	// Since partitions are created at time.Now(), we to advance the test mocked time
	// so that we're outside the lookback window (otherwise there's no caching involved).
	now := time.Now().Add(2 * time.Hour)

	// Start the workers.
	wg := sync.WaitGroup{}
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()

			// Get the subring once. This is the one expected from subsequent requests.
			userID := fmt.Sprintf("user-%d", workerID)
			expected, err := ring.ShuffleShardWithLookback(userID, shardSize, lookback, now)
			require.NoError(t, err)

			for r := 0; r < numRequestsPerWorker; r++ {
				actual, err := ring.ShuffleShardWithLookback(userID, shardSize, lookback, now)
				require.NoError(t, err)
				require.Equal(t, expected, actual)

				// Get the subring for a new user each time too, in order to stress the setter too
				// (if we only read from the cache there's no read/write concurrent access).
				_, err = ring.ShuffleShardWithLookback(fmt.Sprintf("stress-%d", r), 3, lookback, now)
				require.NoError(t, err)
			}
		}(w)
	}

	// Wait until all workers have done.
	wg.Wait()

	// Ensure the cache was populated.
	assert.NotEmpty(t, ring.shuffleShardCache.cacheWithLookback)
	assert.Empty(t, ring.shuffleShardCache.cacheWithoutLookback)
}

func TestPartitionRingGetTokenRangesForPartition(t *testing.T) {
	gen := initTokenGenerator(t)

	tests := map[string]struct {
		partitionTokens map[int][]uint32
		expected        map[int]TokenRanges
	}{
		"single instance in zone": {
			partitionTokens: map[int][]uint32{
				0: gen.GenerateTokens(512, nil),
			},
			expected: map[int]TokenRanges{
				0: {0, math.MaxUint32},
			},
		},
		"simple ranges": {
			partitionTokens: map[int][]uint32{
				0: {25, 75},
				1: {10, 50, 100},
			},
			expected: map[int]TokenRanges{
				0: {10, 24, 50, 74},
				1: {0, 9, 25, 49, 75, math.MaxUint32},
			},
		},
		"grouped tokens": {
			partitionTokens: map[int][]uint32{
				0: {10, 20, 30, 40, 50},
				1: {1000, 2000, 3000, 4000},
			},
			expected: map[int]TokenRanges{
				0: {0, 49, 4000, math.MaxUint32},
				1: {50, 3999},
			},
		},
		"consecutive tokens": {
			partitionTokens: map[int][]uint32{
				0: {99},
				1: {100},
			},
			expected: map[int]TokenRanges{
				0: {0, 98, 100, math.MaxUint32},
				1: {99, 99},
			},
		},
		"extremes": {
			partitionTokens: map[int][]uint32{
				0: {0},
				1: {math.MaxUint32},
			},
			expected: map[int]TokenRanges{
				0: {math.MaxUint32, math.MaxUint32},
				1: {0, math.MaxUint32 - 1},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ringDesc := PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{},
			}

			for p, t := range testData.partitionTokens {
				ringDesc.Partitions[int32(p)] = PartitionDesc{
					Tokens: t,
				}
			}

			r := NewPartitionRing(ringDesc)

			for id, exp := range testData.expected {
				ranges, err := r.GetTokenRangesForPartition(int32(id))
				require.NoError(t, err)
				assert.Equal(t, exp, ranges)

				// validate that the endpoints of the ranges map to the expected instances
				for _, token := range ranges {
					i := searchToken(r.ringTokens, token)
					tok := r.ringTokens[i]
					p, ok := r.partitionByToken[Token(tok)]
					require.True(t, ok)
					assert.Equal(t, int32(id), p)
				}
			}
		})
	}
}

func TestPartitionRingGetTokenRangesForPartitionExhaustive(t *testing.T) {
	// We don't test with bigger number of partitions, because setup takes too long.
	partitionsCount := []int{1, 3, 9, 27, 81}

	for _, partitions := range partitionsCount {
		t.Run(fmt.Sprintf("%d", partitions), func(t *testing.T) {
			pr := preparePartitionRingWithActivePartitions(partitions)

			for partition := int32(0); partition < int32(partitions); partition++ {
				ranges, err := pr.GetTokenRangesForPartition(partition)
				require.NoError(t, err)

				for rng := 0; rng < len(ranges); rng += 2 {
					pid, err := pr.ActivePartitionForKey(ranges[rng])
					require.NoError(t, err)
					require.Equal(t, partition, pid, "first token in the range")

					pid, err = pr.ActivePartitionForKey(ranges[rng+1])
					require.NoError(t, err)
					require.Equal(t, partition, pid, "last token in the range")

					tokenInTheMiddle := uint32((uint64(ranges[rng]) + uint64(ranges[rng+1])) / 2)
					pid, err = pr.ActivePartitionForKey(tokenInTheMiddle)
					require.NoError(t, err)
					require.Equal(t, partition, pid, "middle token in the range")

					if ranges[rng] > 0 {
						pid, err = pr.ActivePartitionForKey(ranges[rng] - 1)
						require.NoError(t, err)
						require.NotEqual(t, partition, pid, "token just before the range start")
					}

					if ranges[rng+1] < math.MaxUint32 {
						pid, err = pr.ActivePartitionForKey(ranges[rng+1] + 1)
						require.NoError(t, err)
						require.NotEqual(t, partition, pid, "token just after the range end")
					}
				}
			}
		})
	}
}

// These rings are immutable, we can cache them between benchmarks.
// It makes benchmarks run faster, esp. for big number of partitions, because preparing ring takes long time.
var cachedRings = map[int]*PartitionRing{}

func preparePartitionRingWithActivePartitions(partitions int) *PartitionRing {
	pr := cachedRings[partitions]
	if pr != nil {
		return pr
	}

	prd := NewPartitionRingDesc()

	for pid := 0; pid < partitions; pid++ {
		prd.AddPartition(int32(pid), PartitionActive, time.Time{}) // benchmark doesn't use state or time.
	}
	pr = NewPartitionRing(*prd)
	cachedRings[partitions] = pr
	return pr
}

func BenchmarkPartitionRingGetTokenRangesForPartition(b *testing.B) {
	partitionsCount := []int{1, 3, 9, 27, 81, 243, 729}

	for _, n := range partitionsCount {
		b.Run(fmt.Sprintf("%d partitions", n), func(b *testing.B) {
			benchmarkPartitionRingGetTokenRangesForInstance(b, n)
		})
	}
}

func benchmarkPartitionRingGetTokenRangesForInstance(b *testing.B, partitions int) {
	r := preparePartitionRingWithActivePartitions(partitions)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err := r.GetTokenRangesForPartition(0)
		if err != nil {
			b.Fatal("unexpected error:", err)
		}
	}
}

func TestActivePartitionBatchRing(t *testing.T) {
	t.Run("should implement DoBatchRing", func(*testing.T) {
		_ = DoBatchRing(&ActivePartitionBatchRing{})
	})
}

func TestActivePartitionBatchRing_InstancesCount(t *testing.T) {
	t.Run("should return the number of InstanceState_ACTIVE partitions", func(t *testing.T) {
		activeRing := NewActivePartitionBatchRing(createPartitionRingWithPartitions(10, 3, 2))
		assert.Equal(t, 10, activeRing.InstancesCount())
	})
}

func TestActivePartitionBatchRing_Get(t *testing.T) {
	const numRuns = 1000

	ring := createPartitionRingWithPartitions(10, 5, 5)
	activeRing := NewActivePartitionBatchRing(ring)
	buf := [GetBufferSize]*InstanceDesc{}

	t.Run("should return a ReplicationSet with the active partition owning the input key", func(t *testing.T) {
		for _, withBuffer := range []bool{true, false} {
			t.Run(fmt.Sprintf("buffer: %t", withBuffer), func(t *testing.T) {
				// Randomise the seed but log it in case we need to reproduce the test on failure.
				seed := time.Now().UnixNano()
				rnd := rand.New(rand.NewSource(seed))
				t.Log("random generator seed:", seed)

				var getBuf []*InstanceDesc
				if withBuffer {
					getBuf = buf[:0]
				}

				for i := 0; i < numRuns; i++ {
					key := uint32(rnd.Intn(math.MaxUint32))
					expected, err := ring.ActivePartitionForKey(key)
					require.NoError(t, err)

					actual, err := activeRing.Get(key, WriteNoExtend, getBuf, nil, nil)
					require.NoError(t, err)

					require.Len(t, actual.Instances, 1)
					assert.Equal(t, strconv.Itoa(int(expected)), actual.Instances[0].Id)
					assert.Equal(t, strconv.Itoa(int(expected)), actual.Instances[0].Addr)
					assert.Equal(t, 0, actual.MaxErrors)
					assert.Equal(t, 0, actual.MaxUnavailableZones)
					assert.False(t, actual.ZoneAwarenessEnabled)
				}
			})
		}
	})

	t.Run("should not allocate memory with buffer is provided", func(t *testing.T) {
		allocs := testing.AllocsPerRun(numRuns, func() {
			_, err := activeRing.Get(12345, WriteNoExtend, buf[:0], nil, nil)
			if err != nil {
				t.Fatal(err)
			}
		})

		require.Equal(t, 0.0, allocs)
	})
}

func BenchmarkActivePartitionBatchRing_Get(b *testing.B) {
	benchCases := map[string]struct {
		ring *ActivePartitionBatchRing
	}{
		"InstanceState_ACTIVE partitions only": {
			ring: NewActivePartitionBatchRing(createPartitionRingWithPartitions(100, 0, 0)),
		},
		"InstanceState_ACTIVE and INACTIVE partitions": {
			ring: NewActivePartitionBatchRing(createPartitionRingWithPartitions(100, 10, 0)),
		},
		"InstanceState_ACTIVE, INACTIVE and InstanceState_PENDING partitions": {
			ring: NewActivePartitionBatchRing(createPartitionRingWithPartitions(100, 10, 10)),
		},
	}

	for benchName, benchCase := range benchCases {
		b.Run(benchName, func(b *testing.B) {
			buf := [GetBufferSize]*InstanceDesc{}

			for n := 0; n < b.N; n++ {
				set, err := benchCase.ring.Get(uint32(n), WriteNoExtend, buf[:0], nil, nil)
				if err != nil {
					b.Fatal(err)
				}
				if got, expected := len(set.Instances), 1; got != expected {
					b.Fatalf("unexpected number of partitions (got: %d, expected: %d)", got, expected)
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

func createPartitionRingWithPartitions(numActive, numInactive, numPending int) *PartitionRing {
	desc := NewPartitionRingDesc()

	for i := 0; i < numActive; i++ {
		desc.AddPartition(int32(i), PartitionActive, time.Now())
	}
	for i := numActive; i < numActive+numInactive; i++ {
		desc.AddPartition(int32(i), PartitionInactive, time.Now())
	}
	for i := numActive + numInactive; i < numActive+numInactive+numPending; i++ {
		desc.AddPartition(int32(i), PartitionPending, time.Now())
	}

	return NewPartitionRing(*desc)
}

// comparePartitionIDs returns the list of partition IDs which differ between the two lists.
func comparePartitionIDs(first, second []int32) (added, removed []int32) {
	for _, id := range first {
		if !slices.Contains(second, id) {
			removed = append(removed, id)
		}
	}

	for _, id := range second {
		if !slices.Contains(first, id) {
			added = append(added, id)
		}
	}

	return
}

func randomString(rnd *rand.Rand, length int) string {
	const (
		firstASCIIChar = '0'
		lastASCIIChar  = 'Z'
	)

	out := make([]byte, 0, length)
	for i := 0; i < length; i++ {
		out = append(out, byte(firstASCIIChar+rnd.Intn(lastASCIIChar-firstASCIIChar)))
	}
	return string(out)
}
