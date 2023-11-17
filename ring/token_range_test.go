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
	ranges := []uint32{4, 8, 12, 16}

	require.False(t, KeyInTokenRanges(0, ranges))
	require.True(t, KeyInTokenRanges(4, ranges))
	require.True(t, KeyInTokenRanges(6, ranges))
	require.True(t, KeyInTokenRanges(8, ranges))
	require.False(t, KeyInTokenRanges(10, ranges))
	require.False(t, KeyInTokenRanges(20, ranges))
}

func TestGetTokenRangesForInstance(t *testing.T) {
	numZones := 3

	tests := map[string]struct {
		zoneTokens map[string][]uint32
		expected   map[string][]uint32
	}{
		"single instance in zone": {
			zoneTokens: map[string][]uint32{
				"instance-0-0": GenerateTokens(512, nil),
			},
			expected: map[string][]uint32{
				"instance-0-0": {0, math.MaxUint32},
			},
		},
		"simple ranges": {
			zoneTokens: map[string][]uint32{
				"instance-0-0": {25, 75},
				"instance-0-1": {10, 50, 100},
			},
			expected: map[string][]uint32{
				"instance-0-0": {10, 24, 50, 74},
				"instance-0-1": {0, 9, 25, 49, 75, math.MaxUint32},
			},
		},
		"grouped tokens": {
			zoneTokens: map[string][]uint32{
				"instance-0-0": {10, 20, 30, 40, 50},
				"instance-0-1": {1000, 2000, 3000, 4000},
			},
			expected: map[string][]uint32{
				"instance-0-0": {0, 49, 4000, math.MaxUint32},
				"instance-0-1": {50, 3999},
			},
		},
		"consecutive tokens": {
			zoneTokens: map[string][]uint32{
				"instance-0-0": {99},
				"instance-0-1": {100},
			},
			expected: map[string][]uint32{
				"instance-0-0": {0, 98, 100, math.MaxUint32},
				"instance-0-1": {99, 99},
			},
		},
		"extremes": {
			zoneTokens: map[string][]uint32{
				"instance-0-0": {0},
				"instance-0-1": {math.MaxUint32},
			},
			expected: map[string][]uint32{
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
