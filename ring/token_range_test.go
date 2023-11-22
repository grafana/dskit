package ring

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyInTokenRanges(t *testing.T) {
	ranges := TokenRanges{4, 8, 12, 16}

	require.False(t, ranges.IncludesKey(0))
	require.True(t, ranges.IncludesKey(4))
	require.True(t, ranges.IncludesKey(6))
	require.True(t, ranges.IncludesKey(8))
	require.False(t, ranges.IncludesKey(10))
	require.False(t, ranges.IncludesKey(20))
}

func TestGetTokenRangesForInstance(t *testing.T) {
	numZones := 3

	tests := map[string]struct {
		zoneTokens map[string][]uint32
		expected   map[string]TokenRanges
	}{
		"single instance in zone": {
			zoneTokens: map[string][]uint32{
				"instance-0-0": GenerateTokens(512, nil),
			},
			expected: map[string]TokenRanges{
				"instance-0-0": {0, math.MaxUint32},
			},
		},
		"simple ranges": {
			zoneTokens: map[string][]uint32{
				"instance-0-0": {25, 75},
				"instance-0-1": {10, 50, 100},
			},
			expected: map[string]TokenRanges{
				"instance-0-0": {10, 24, 50, 74},
				"instance-0-1": {0, 9, 25, 49, 75, math.MaxUint32},
			},
		},
		"grouped tokens": {
			zoneTokens: map[string][]uint32{
				"instance-0-0": {10, 20, 30, 40, 50},
				"instance-0-1": {1000, 2000, 3000, 4000},
			},
			expected: map[string]TokenRanges{
				"instance-0-0": {0, 49, 4000, math.MaxUint32},
				"instance-0-1": {50, 3999},
			},
		},
		"consecutive tokens": {
			zoneTokens: map[string][]uint32{
				"instance-0-0": {99},
				"instance-0-1": {100},
			},
			expected: map[string]TokenRanges{
				"instance-0-0": {0, 98, 100, math.MaxUint32},
				"instance-0-1": {99, 99},
			},
		},
		"extremes": {
			zoneTokens: map[string][]uint32{
				"instance-0-0": {0},
				"instance-0-1": {math.MaxUint32},
			},
			expected: map[string]TokenRanges{
				"instance-0-0": {math.MaxUint32, math.MaxUint32},
				"instance-0-1": {0, math.MaxUint32 - 1},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			instances := map[string]InstanceDesc{}
			allTokens := []uint32{}

			// generate test zone
			for id, tokens := range testData.zoneTokens {
				instances[id] = generateRingInstanceWithInfo(id, "zone-0", tokens, time.Now())
				allTokens = append(allTokens, tokens...)
			}

			// generate other zones
			for z := 1; z < numZones; z++ {
				for i := 0; i < len(testData.zoneTokens); i++ {
					id := fmt.Sprintf("instance-%d-%d", z, i)
					tokens := GenerateTokens(512, allTokens)
					instances[id] = generateRingInstanceWithInfo(id, fmt.Sprintf("zone-%d", z), tokens, time.Now())
					allTokens = append(allTokens, tokens...)
				}
			}

			// Initialise the ring.
			ringDesc := &Desc{Ingesters: instances}
			ring := Ring{
				cfg:                  Config{HeartbeatTimeout: time.Hour, ZoneAwarenessEnabled: true, SubringCacheDisabled: true, ReplicationFactor: numZones},
				ringDesc:             ringDesc,
				ringTokens:           ringDesc.GetTokens(),
				ringTokensByZone:     ringDesc.getTokensByZone(),
				ringInstanceByToken:  ringDesc.getTokensInfo(),
				ringZones:            getZones(ringDesc.getTokensByZone()),
				shuffledSubringCache: map[subringCacheKey]*Ring{},
				strategy:             NewDefaultReplicationStrategy(),
				lastTopologyChange:   time.Now(),
			}

			for id, exp := range testData.expected {
				ranges, err := ring.GetTokenRangesForInstance(id)
				require.NoError(t, err)
				assert.Equal(t, exp, ranges)

				// validate that the endpoints of the ranges map to the expected instances
				for _, token := range ranges {
					zoneTokens := ring.ringTokensByZone["zone-0"]
					i := searchToken(zoneTokens, token)
					assert.Equal(t, id, ring.ringInstanceByToken[zoneTokens[i]].InstanceID)
				}
			}
		})
	}
}

func BenchmarkGetTokenRangesForInstance(b *testing.B) {
	instancesPerZone := []int{1, 3, 9, 27, 81, 243, 729}

	for _, n := range instancesPerZone {
		b.Run(fmt.Sprintf("%d_instancesperzone", n), func(b *testing.B) {
			benchmarkGetTokenRangesForInstance(b, n)
		})
	}
}

func benchmarkGetTokenRangesForInstance(b *testing.B, instancesPerZone int) {
	numZones := 3
	numTokens := 512

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		// Initialise the ring.
		ringDesc := &Desc{Ingesters: generateRingInstances(instancesPerZone*numZones, numZones, numTokens)}
		ring := Ring{
			cfg:                  Config{HeartbeatTimeout: time.Hour, ZoneAwarenessEnabled: true, SubringCacheDisabled: true, ReplicationFactor: numZones},
			ringDesc:             ringDesc,
			ringTokens:           ringDesc.GetTokens(),
			ringTokensByZone:     ringDesc.getTokensByZone(),
			ringInstanceByToken:  ringDesc.getTokensInfo(),
			ringZones:            getZones(ringDesc.getTokensByZone()),
			shuffledSubringCache: map[subringCacheKey]*Ring{},
			strategy:             NewDefaultReplicationStrategy(),
			lastTopologyChange:   time.Now(),
		}
		b.StartTimer()

		_, _ = ring.GetTokenRangesForInstance("instance-1")
	}
}

func TestCheckingOfKeyOwnership(t *testing.T) {
	const instancesPerZone = 100
	const numZones = 3
	const numTokens = 512
	const replicationFactor = numZones // This is the only config supported by GetTokenRangesForInstance right now.

	// Generate users with different number of tokens
	userTokens := map[string][]uint32{}
	shardSizes := map[string]int{}
	for _, cnt := range []int{1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000} {
		uid := fmt.Sprintf("%dk", cnt/1000)
		userTokens[uid] = GenerateTokens(cnt, nil)

		shardSize := cnt / 7500
		shardSize = (shardSize / numZones) * numZones // round down to numZones
		if shardSize < numZones {
			shardSize = numZones
		}
		shardSizes[uid] = shardSize
	}

	// Generate ring
	ringDesc := &Desc{Ingesters: generateRingInstances(instancesPerZone*numZones, numZones, numTokens)}
	ring := Ring{
		cfg:                  Config{HeartbeatTimeout: time.Hour, ZoneAwarenessEnabled: true, SubringCacheDisabled: false, ReplicationFactor: replicationFactor},
		ringDesc:             ringDesc,
		ringTokens:           ringDesc.GetTokens(),
		ringTokensByZone:     ringDesc.getTokensByZone(),
		ringInstanceByToken:  ringDesc.getTokensInfo(),
		ringZones:            getZones(ringDesc.getTokensByZone()),
		shuffledSubringCache: map[subringCacheKey]*Ring{},
		strategy:             NewDefaultReplicationStrategy(),
		lastTopologyChange:   time.Now(),
	}

	for uid, tokens := range userTokens {
		shardSize := shardSizes[uid]

		subRing := ring.ShuffleShard(uid, shardSize)
		sr := subRing.(*Ring)

		// find some instance in subring
		var instanceID string
		for id, _ := range sr.ringDesc.Ingesters {
			instanceID = id
			break
		}

		// Compute owned tokens by using token ranges.
		ranges, err := subRing.GetTokenRangesForInstance(instanceID)
		require.NoError(t, err)

		cntViaTokens := 0
		for _, t := range tokens {
			if ranges.IncludesKey(t) {
				cntViaTokens++
			}
		}

		// Compute owned tokens using numberOfKeysOwnedByInstance.
		bufDescs := make([]InstanceDesc, 5)
		bufHosts := make([]string, 5)
		bufZones := make([]string, numZones)

		cntViaGet, err := sr.numberOfKeysOwnedByInstance(tokens, WriteNoExtend, instanceID, bufDescs, bufHosts, bufZones)
		require.NoError(t, err)

		assert.Equal(t, cntViaTokens, cntViaGet)
	}
}

func BenchmarkCompareCountingOfSeriesViaRingAndTokenRanges(b *testing.B) {
	const instancesPerZone = 100
	const numZones = 3
	const numTokens = 512
	const userTokens = 500000
	const userShardsize = 60

	seriesTokens := GenerateTokens(userTokens, nil)

	// Generate ring
	ringDesc := &Desc{Ingesters: generateRingInstances(instancesPerZone*numZones, numZones, numTokens)}
	ring := Ring{
		cfg:                  Config{HeartbeatTimeout: time.Hour, ZoneAwarenessEnabled: true, SubringCacheDisabled: false, ReplicationFactor: numZones},
		ringDesc:             ringDesc,
		ringTokens:           ringDesc.GetTokens(),
		ringTokensByZone:     ringDesc.getTokensByZone(),
		ringInstanceByToken:  ringDesc.getTokensInfo(),
		ringZones:            getZones(ringDesc.getTokensByZone()),
		shuffledSubringCache: map[subringCacheKey]*Ring{},
		strategy:             NewDefaultReplicationStrategy(),
		lastTopologyChange:   time.Now(),
	}

	// compute and cache subrings for each user
	subRing := ring.ShuffleShard("user", userShardsize)
	sr := subRing.(*Ring)

	// find some instance in subring
	var instanceID string
	for id, _ := range sr.ringDesc.Ingesters {
		instanceID = id
		break
	}

	b.Run("GetTokenRangesForInstance", func(b *testing.B) {
		tokenRange, err := subRing.GetTokenRangesForInstance(instanceID)
		require.NoError(b, err)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cntViaTokens := 0
			for _, t := range seriesTokens {
				if tokenRange.IncludesKey(t) {
					cntViaTokens++
				}
			}
			if cntViaTokens <= 0 {
				b.Fatal("no owned tokens found!")
			}
		}
	})

	b.Run("numberOfKeysOwnedByInstance", func(b *testing.B) {
		bufDescs := make([]InstanceDesc, 5)
		bufHosts := make([]string, 5)
		bufZones := make([]string, numZones)

		for i := 0; i < b.N; i++ {
			cntViaGet, err := sr.numberOfKeysOwnedByInstance(seriesTokens, WriteNoExtend, instanceID, bufDescs, bufHosts, bufZones)
			require.NoError(b, err)

			if cntViaGet <= 0 {
				b.Fatal("no owned tokens found!")
			}
		}
	})
}
