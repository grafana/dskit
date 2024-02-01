package ring

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
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

func TestPartitionRing_ShuffleShard(t *testing.T) {
	t.Run("should honor the shard size", func(t *testing.T) {
		const numActivePartitions = 5

		ring := createPartitionRingWithPartitions(numActivePartitions, 0)

		// Request a shard size up to the number of existing partitions.
		for shardSize := 1; shardSize <= numActivePartitions; shardSize++ {
			subring, err := ring.ShuffleShard("tenant-id", shardSize)
			require.NoError(t, err)
			assert.Equal(t, shardSize, subring.PartitionsCount())
		}

		// Request a shard size greater than the number of existing partitions.
		subring, err := ring.ShuffleShard("tenant-id", numActivePartitions+1)
		require.NoError(t, err)
		assert.Equal(t, numActivePartitions, subring.PartitionsCount())
	})

	// TODO logic around inactive partitions
}

// This test asserts on shard stability across multiple invocations and given the same input ring.
func TestPartitionRing_ShuffleShard_Stability(t *testing.T) {
	var (
		numTenants            = 100
		numActivePartitions   = 50
		numInactivePartitions = 10
		numInvocations        = 10
		shardSizes            = []int{3, 6, 9, 12, 15}
	)

	// Initialise the ring.
	ring := createPartitionRingWithPartitions(numActivePartitions, numInactivePartitions)

	for i := 1; i <= numTenants; i++ {
		tenantID := fmt.Sprintf("%d", i)

		for _, size := range shardSizes {
			expectedSubring, err := ring.ShuffleShard(tenantID, size)
			require.NoError(t, err)

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
	ring := createPartitionRingWithPartitions(numActivePartitions, 0)

	// Compute the shard for each tenant.
	partitionsByTenant := map[string][]int32{}

	for i := 1; i <= numTenants; i++ {
		tenantID := fmt.Sprintf("%d", i)

		subring, err := ring.ShuffleShard(tenantID, shardSize)
		require.NoError(t, err)

		partitionsByTenant[tenantID] = subring.ActivePartitionIDs()
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

// TODO test with inactive partitions too
func TestPartitionRing_ShuffleShard_ConsistencyOnPartitionsTopologyChange(t *testing.T) {
	type change string

	type scenario struct {
		name          string
		numPartitions int
		shardSize     int
		ringChange    change
	}

	const (
		numTenants = 100
		add        = change("add-partition")
		remove     = change("remove-partition")
	)

	// Generate all test scenarios.
	var scenarios []scenario
	for _, numPartitions := range []int{20, 30, 40, 50} {
		for _, shardSize := range []int{3, 6, 9, 12, 15} {
			for _, c := range []change{add, remove} {
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
		t.Run(s.name, func(t *testing.T) {
			ring := createPartitionRingWithPartitions(s.numPartitions, 0)

			// Compute the initial shard for each tenant.
			initial := map[int][]int32{}
			for id := 0; id < numTenants; id++ {
				subring, err := ring.ShuffleShard(fmt.Sprintf("%d", id), s.shardSize)
				require.NoError(t, err)
				// TODO we should compare all partitions when adding inactive too
				initial[id] = subring.ActivePartitionIDs()
			}

			// Update the ring.
			switch s.ringChange {
			case add:
				desc := &(ring.desc)
				desc.AddPartition(int32(s.numPartitions+1), PartitionActive, time.Now())
				ring = NewPartitionRing(*desc)
			case remove:
				// Remove the first one.
				desc := &(ring.desc)
				desc.RemovePartition(ring.ActivePartitionIDs()[0])
				ring = NewPartitionRing(*desc)
			}

			// Compute the updated shard for each tenant and compare it with the initial one.
			// If the "consistency" property is guaranteed, we expect no more than 1 different
			// partition in the updated shard.
			for id := 0; id < numTenants; id++ {
				subring, err := ring.ShuffleShard(fmt.Sprintf("%d", id), s.shardSize)
				require.NoError(t, err)

				// TODO we should compare all partitions when adding inactive too
				updated := subring.ActivePartitionIDs()

				added, removed := comparePartitionIDs(initial[id], updated)
				assert.LessOrEqual(t, len(added), 1)
				assert.LessOrEqual(t, len(removed), 1)
			}
		})
	}
}

func TestPartitionRing_ShuffleShard_ConsistencyOnShardSizeChanged(t *testing.T) {
	ring := createPartitionRingWithPartitions(30, 0)

	// Get the replication set with shard size = 3.
	firstShard, err := ring.ShuffleShard("tenant-id", 3)
	require.NoError(t, err)
	assert.Equal(t, 3, firstShard.PartitionsCount())
	firstPartitions := firstShard.ActivePartitionIDs()

	// Increase shard size to 6.
	secondShard, err := ring.ShuffleShard("tenant-id", 6)
	require.NoError(t, err)
	assert.Equal(t, 6, secondShard.PartitionsCount())
	secondPartitions := secondShard.ActivePartitionIDs()

	for _, id := range firstPartitions {
		assert.True(t, slices.Contains(secondPartitions, id), "new shard is expected to include previous partition %s", id)
	}

	// Increase shard size to 9.
	thirdShard, err := ring.ShuffleShard("tenant-id", 9)
	require.NoError(t, err)
	assert.Equal(t, 9, thirdShard.PartitionsCount())
	thirdPartitions := thirdShard.ActivePartitionIDs()

	for _, id := range secondPartitions {
		assert.True(t, slices.Contains(thirdPartitions, id), "new shard is expected to include previous partition %s", id)
	}

	// Decrease shard size to 6.
	fourthShard, err := ring.ShuffleShard("tenant-id", 6)
	require.NoError(t, err)
	assert.Equal(t, 6, fourthShard.PartitionsCount())
	fourthPartitions := fourthShard.ActivePartitionIDs()

	// We expect to have the same exact instances we had when the shard size was 6.
	assert.Equal(t, secondPartitions, fourthPartitions)

	// Decrease shard size to 3.
	fifthShard, err := ring.ShuffleShard("tenant-id", 3)
	require.NoError(t, err)
	assert.Equal(t, 3, fifthShard.PartitionsCount())
	fifthPartitions := fifthShard.ActivePartitionIDs()

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
				// Partition 2 still inactive, but now falls outside the lookback period
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionInactive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}}, // TODO why?
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
				// Partition 2 switches to inactive
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionInactive, withinLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}},
				// Partition 2 still inactive, but now falls outside the lookback period
				{what: update, partitionID: 2, partitionDesc: generatePartitionWithInfo(PartitionInactive, outsideLookback)},
				{what: test, shardSize: 2, expected: []int32{2, 1, 0}}, // TODO why?
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

/* TODO
func TestPartitionRing_ShuffleShardWithLookback_CorrectnessWithFuzzy(t *testing.T) {
	// The goal of this test is NOT to ensure that the minimum required number of partitions
	// are returned at any given time, BUT at least all required partitions are returned.
	var (
		numInitialInstances = []int{9, 30, 60, 90}
		numInitialZones     = []int{1, 3}
		numEvents           = 100
		lookbackPeriod      = time.Hour
		delayBetweenEvents  = 5 * time.Minute // 12 events / hour
		userID              = "user-1"
	)

	for _, updateOldestRegisteredTimestamp := range []bool{false, true} {
		updateOldestRegisteredTimestamp := updateOldestRegisteredTimestamp

		for _, numInstances := range numInitialInstances {
			numInstances := numInstances

			for _, numZones := range numInitialZones {
				numZones := numZones

				testName := fmt.Sprintf("num instances = %d, num zones = %d, update oldest registered timestamp = %v", numInstances, numZones, updateOldestRegisteredTimestamp)

				t.Run(testName, func(t *testing.T) {
					t.Parallel()

					// Randomise the seed but log it in case we need to reproduce the test on failure.
					seed := time.Now().UnixNano()
					rnd := rand.New(rand.NewSource(seed))
					t.Log("random generator seed:", seed)
					gen := NewRandomTokenGeneratorWithSeed(seed)

					// Initialise the ring.
					ringDesc := &Desc{Ingesters: generateRingInstances(initTokenGenerator(t), numInstances, numZones, 128)}
					ring := Ring{
						cfg: Config{
							HeartbeatTimeout:     time.Hour,
							ZoneAwarenessEnabled: true,
							ReplicationFactor:    3,
						},
						ringDesc:            ringDesc,
						ringTokens:          ringDesc.GetTokens(),
						ringTokensByZone:    ringDesc.getTokensByZone(),
						ringInstanceByToken: ringDesc.getTokensInfo(),
						ringZones:           getZones(ringDesc.getTokensByZone()),
						strategy:            NewDefaultReplicationStrategy(),
					}
					if updateOldestRegisteredTimestamp {
						ring.oldestRegisteredTimestamp = ringDesc.getOldestRegisteredTimestamp()
					}

					// The simulation starts with the minimum shard size. Random events can later increase it.
					shardSize := numZones

					// The simulation assumes the initial ring contains instances registered
					// since more than the lookback period.
					currTime := time.Now().Add(lookbackPeriod).Add(time.Minute)

					// Add the initial shard to the history.
					rs, err := ring.shuffleShard(userID, shardSize, 0, time.Now()).GetReplicationSetForOperation(Read)
					require.NoError(t, err)

					history := map[time.Time]ReplicationSet{
						currTime: rs,
					}

					// Simulate a progression of random events over the time and, at each iteration of the simuation,
					// make sure the subring includes all non-removed instances picked from previous versions of the
					// ring up until the lookback period.
					nextInstanceID := len(ringDesc.Ingesters) + 1

					for i := 1; i <= numEvents; i++ {
						currTime = currTime.Add(delayBetweenEvents)

						switch r := rnd.Intn(100); {
						case r < 80:
							// Scale up instances by 1.
							instanceID := fmt.Sprintf("instance-%d", nextInstanceID)
							zoneID := fmt.Sprintf("zone-%d", nextInstanceID%numZones)
							nextInstanceID++

							ringDesc.Ingesters[instanceID] = generateRingInstanceWithInfo(instanceID, zoneID, gen.GenerateTokens(128, nil), currTime)

							ring.ringTokens = ringDesc.GetTokens()
							ring.ringTokensByZone = ringDesc.getTokensByZone()
							ring.ringInstanceByToken = ringDesc.getTokensInfo()
							ring.ringZones = getZones(ringDesc.getTokensByZone())
							if updateOldestRegisteredTimestamp {
								ring.oldestRegisteredTimestamp = ringDesc.getOldestRegisteredTimestamp()
							}
						case r < 90:
							// Scale down instances by 1. To make tests reproducible we get the instance IDs, sort them
							// and then get a random index (using the random generator initialized with a constant seed).
							instanceIDs := make([]string, 0, len(ringDesc.Ingesters))
							for id := range ringDesc.Ingesters {
								instanceIDs = append(instanceIDs, id)
							}

							sort.Strings(instanceIDs)

							idxToRemove := rnd.Intn(len(instanceIDs))
							idToRemove := instanceIDs[idxToRemove]
							delete(ringDesc.Ingesters, idToRemove)

							ring.ringTokens = ringDesc.GetTokens()
							ring.ringTokensByZone = ringDesc.getTokensByZone()
							ring.ringInstanceByToken = ringDesc.getTokensInfo()
							ring.ringZones = getZones(ringDesc.getTokensByZone())
							if updateOldestRegisteredTimestamp {
								ring.oldestRegisteredTimestamp = ringDesc.getOldestRegisteredTimestamp()
							}

							// Remove the terminated instance from the history.
							for ringTime, ringState := range history {
								for idx, desc := range ringState.Instances {
									// In this simulation instance ID == instance address.
									if desc.Addr != idToRemove {
										continue
									}

									ringState.Instances = append(ringState.Instances[:idx], ringState.Instances[idx+1:]...)
									history[ringTime] = ringState
									break
								}
							}
						default:
							// Scale up shard size (keeping the per-zone balance).
							shardSize += numZones
						}

						// Add the current shard to the history.
						rs, err = ring.shuffleShard(userID, shardSize, 0, time.Now()).GetReplicationSetForOperation(Read)
						require.NoError(t, err)
						history[currTime] = rs

						// Ensure the shard with lookback includes all instances from previous states of the ring.
						rsWithLookback, err := ring.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, currTime).GetReplicationSetForOperation(Read)
						require.NoError(t, err)

						for ringTime, ringState := range history {
							if ringTime.Before(currTime.Add(-lookbackPeriod)) {
								// This entry from the history is obsolete, we can remove it.
								delete(history, ringTime)
								continue
							}

							for _, expectedAddr := range ringState.GetAddresses() {
								if !rsWithLookback.Includes(expectedAddr) {
									t.Fatalf(
										"subring generated after event %d is expected to include instance %s from ring state at time %s but it's missing (actual instances are: %s)",
										i, expectedAddr, ringTime.String(), strings.Join(rsWithLookback.GetAddresses(), ", "))
								}
							}
						}
					}
				})
			}
		}
	}
}
*/

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
		require.Equal(t, []int32{2, 3, 5}, first.ActivePartitionIDs())
	})

	t.Run("different shard size", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(tenantID, subringSize, time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(tenantID, subringSize+1, time.Minute, now)
		require.NoError(t, err)

		require.NotSame(t, first, second)
		require.Equal(t, []int32{2, 3, 5}, first.ActivePartitionIDs())
		require.Equal(t, []int32{1, 2, 3, 5}, second.ActivePartitionIDs())
	})

	t.Run("different identifiers", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(tenantID, subringSize, time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, time.Minute, now)
		require.NoError(t, err)

		require.NotSame(t, first, second)
		require.Equal(t, []int32{2, 3, 5}, first.ActivePartitionIDs())
		require.Equal(t, []int32{1, 4, 6}, second.ActivePartitionIDs())
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
		require.Equal(t, []int32{2, 3, 5}, first.ActivePartitionIDs())
		require.Equal(t, []int32{2, 3, 5}, second.ActivePartitionIDs())
		require.Equal(t, []int32{1, 2, 3, 4, 5}, third.ActivePartitionIDs())
	})

	t.Run("same lookback window at different times, both over a period touching the most recent partition state change", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 80*time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 80*time.Minute, now.Add(5*time.Minute))
		require.NoError(t, err)

		require.Same(t, first, second)
		require.Equal(t, []int32{1, 2, 4, 6}, first.ActivePartitionIDs())
		require.Equal(t, []int32{1, 2, 4, 6}, second.ActivePartitionIDs())
	})

	t.Run("same lookback window at different times, crossing the period of the most recent partition state change", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 80*time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 80*time.Minute, now.Add(30*time.Minute))
		require.NoError(t, err)

		require.NotSame(t, first, second)
		require.Equal(t, []int32{1, 2, 4, 6}, first.ActivePartitionIDs())
		require.Equal(t, []int32{1, 4, 6}, second.ActivePartitionIDs())
	})

	t.Run("same lookback window at different times, crossing the period of the 2 most recent partition state changes", func(t *testing.T) {
		first, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 100*time.Minute, now)
		require.NoError(t, err)

		second, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 100*time.Minute, now.Add(60*time.Minute))
		require.NoError(t, err)

		require.NotSame(t, first, second)
		require.Equal(t, []int32{1, 2, 3, 4, 6}, first.ActivePartitionIDs())
		require.Equal(t, []int32{1, 4, 6}, second.ActivePartitionIDs())
	})

	t.Run("same lookback window at different alternate times, crossing the period of the most recent partition state change", func(t *testing.T) {
		firstEarly, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 62*time.Minute, now)
		require.NoError(t, err)

		firstLate, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 62*time.Minute, now.Add(5*time.Minute))
		require.NoError(t, err)

		require.NotSame(t, firstEarly, firstLate)
		require.Equal(t, []int32{1, 2, 4, 6}, firstEarly.ActivePartitionIDs())
		require.Equal(t, []int32{1, 4, 6}, firstLate.ActivePartitionIDs())

		// Run again the same requests.
		secondEarly, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 62*time.Minute, now)
		require.NoError(t, err)

		secondLate, err := ring.ShuffleShardWithLookback(otherTenantID, subringSize, 62*time.Minute, now.Add(5*time.Minute))
		require.NoError(t, err)

		require.NotSame(t, secondEarly, secondLate)
		require.Equal(t, firstEarly.ActivePartitionIDs(), secondEarly.ActivePartitionIDs())
		require.Equal(t, firstLate.ActivePartitionIDs(), secondLate.ActivePartitionIDs())

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
		require.Equal(t, []int32{1, 2, 3, 4, 5, 6}, first.ActivePartitionIDs())
		require.Equal(t, []int32{1, 2, 3, 4, 5, 6}, second.ActivePartitionIDs())
	})
}

func TestPartitionRing_ShuffleShardWithLookback_CachingConcurrency(t *testing.T) {
	const (
		numWorkers            = 10
		numRequestsPerWorker  = 1000
		numActivePartitions   = 100
		numInactivePartitions = 10
		shardSize             = 3
		lookback              = time.Hour
	)

	ring := createPartitionRingWithPartitions(numActivePartitions, numInactivePartitions)

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

func generatePartitionWithInfo(state PartitionState, stateTS time.Time) PartitionDesc {
	return PartitionDesc{
		State:          state,
		StateTimestamp: stateTS.Unix(),
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
