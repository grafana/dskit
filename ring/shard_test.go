package ring

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	shardUtil "github.com/grafana/dskit/ring/shard"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

const (
	testingCell = "cp10-20240616"
	testingDir  = ""
)

func createRealRing(t *testing.T, cell string) *Ring {
	ringDesc, err := GetRealRingDesc(testingDir, cell)
	require.NoError(t, err)
	return &Ring{
		cfg: Config{
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
}

func createRingWithSpreadMinimizingTokens(t *testing.T, zones []string, ingestersPerZone int) *Ring {
	ringDesc := &Desc{}
	for _, zone := range zones {
		instance := fmt.Sprintf("ingester-%s-%d", zone, ingestersPerZone)
		tokenGenerator, err := NewSpreadMinimizingTokenGenerator(instance, zone, zones, false)
		require.NoError(t, err)
		tokensByInstanceID, err := tokenGenerator.generateTokensByInstanceID()
		require.NoError(t, err)
		for i := 0; i < ingestersPerZone; i++ {
			instanceID := fmt.Sprintf("ingester-%s-%d", zone, i)
			tokens := tokensByInstanceID[i]
			slices.Sort(tokens)
			ringDesc.AddIngester(instanceID, instanceID, zone, tokens, ACTIVE, time.Now())
		}
	}
	return &Ring{
		cfg: Config{
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
}

type tenantInfo struct {
	tenantID        string
	timeseriesCount int
	maxGlobalLimit  int
	shardSize       int
}

type byTenantInfo []tenantInfo

func (ti byTenantInfo) Len() int      { return len(ti) }
func (ti byTenantInfo) Swap(i, j int) { ti[i], ti[j] = ti[j], ti[i] }
func (ti byTenantInfo) Less(i, j int) bool {
	if ti[i].timeseriesCount < ti[j].timeseriesCount {
		return true
	}
	if ti[i].timeseriesCount == ti[j].timeseriesCount {
		return ti[i].tenantID < ti[j].tenantID
	}
	return false
}

type instInfo struct {
	instanceID string
	shardSize  int
	count      int
}

type byInstanceInfo []instInfo

func (ii byInstanceInfo) Len() int      { return len(ii) }
func (ii byInstanceInfo) Swap(i, j int) { ii[i], ii[j] = ii[j], ii[i] }
func (ii byInstanceInfo) Less(i, j int) bool {
	if ii[i].count > ii[j].count {
		return true
	}
	if ii[i].count == ii[j].count {
		if ii[i].instanceID < ii[j].instanceID {
			return true
		}
		if ii[i].instanceID == ii[j].instanceID {
			return ii[i].shardSize > ii[j].shardSize
		}
	}
	return false
}

func TestInputDataByTenant(t *testing.T) {
	shardSizeByTenantID, err := GetShardSizeByTenantID(testingDir, testingCell)
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID(testingDir, testingCell)
	require.NoError(t, err)
	maxGlobalSeriesPerTenant, err := GetMaxGlobalLimitByTenantID(testingDir, testingCell)
	require.NoError(t, err)
	data := make(byTenantInfo, 0, len(shardSizeByTenantID))
	for tenantID, shardSize := range shardSizeByTenantID {
		currTimeseriesCount, ok := timeSeriesCountByTenantID[tenantID]
		if !ok {
			currTimeseriesCount = 0
		}
		maxGlobalLimit, ok := maxGlobalSeriesPerTenant[tenantID]
		if !ok {
			maxGlobalLimit = 15000
		}
		data = append(data, tenantInfo{
			tenantID:        tenantID,
			shardSize:       shardSize,
			timeseriesCount: currTimeseriesCount / 3,
			maxGlobalLimit:  maxGlobalLimit,
		})
	}

	sort.Sort(data)
	for _, tI := range data {
		fmt.Printf("%10s %10d %10d %10d\n", tI.tenantID, tI.timeseriesCount, tI.maxGlobalLimit, tI.shardSize)
	}
}

func TestInputDataSMTShardsByInstance(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	shardSizeByTenantID, err := GetShardSizeByTenantID(testingDir, testingCell)
	require.NoError(t, err)

	slices.Sort(zones)

	stat := make(map[string]map[int]int, ingestersPerZone)

	for tenantID, shardSize := range shardSizeByTenantID {
		random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(tenantID, testingDir)))
		tokens := ring.ringTokensByZone[zones[0]]
		start := searchToken(tokens, random.Uint32())
		// Wrap start around in the ring.
		start %= len(tokens)

		info, ok := ring.ringInstanceByToken[tokens[start]]
		if !ok {
			// This should never happen unless a bug in the ring code.
			panic(ErrInconsistentTokensInfo)
		}

		instanceID := info.InstanceID
		if _, ok := stat[instanceID]; !ok {
			stat[instanceID] = make(map[int]int)
		}
		count, ok := stat[instanceID][shardSize]
		if !ok {
			count = 0
		}
		stat[instanceID][shardSize] = count + 1
	}

	var ii = make(byInstanceInfo, 0, ingestersPerZone*10)
	for instanceID, countByShardSize := range stat {
		for shardSize, count := range countByShardSize {
			ii = append(ii, instInfo{
				instanceID: instanceID,
				shardSize:  shardSize,
				count:      count,
			})
		}
	}

	sort.Sort(ii)
	for _, i := range ii {
		fmt.Printf("%10d %20s %10d\n", i.count, i.instanceID, i.shardSize)
	}
}

func testTimeseriesDistribution(t *testing.T, dir string, cell string, zones []string, ingestersPerZone int, ring *Ring, shardUpdater func(int, int, int) int) {
	shardSizeByTenantID, err := GetShardSizeByTenantID(dir, cell)
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID(dir, cell)
	//timeSeriesCountByTenantID, err := GetMaxGlobalLimitByTenantID(dir, cell)
	require.NoError(t, err)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	timeSeriesCountByInstanceByZone := make(map[string]map[string]int, len(ring.ringZones))
	tenantCountByInstance := make(map[string]int, len(zones)*ingestersPerZone)
	tenantCount := len(timeSeriesCountByTenantID)
	currCount := 0
	for tenantId, timeSeriesCount := range timeSeriesCountByTenantID {
		currCount++
		fmt.Printf("handling tenant: %s (%d out of %d)... ", tenantId, currCount, tenantCount)
		shardSize, ok := shardSizeByTenantID[tenantId]
		if !ok {
			shardSize = 0
		}
		if shardUpdater != nil {
			shardSize = shardUpdater(shardSize, len(zones), ingestersPerZone)
		}
		shard, ok := ring.ShuffleShard(tenantId, shardSize).(*Ring)
		updateTenantCountByInstance(tenantCountByInstance, shard)
		for _, zone := range ring.ringZones {
			tokensFromZone := shard.ringTokensByZone[zone]
			timeSeriesCountByInstance, ok := timeSeriesCountByInstanceByZone[zone]
			if !ok {
				timeSeriesCountByInstance = make(map[string]int, len(ring.ringDesc.Ingesters))
			}
			timeSeriesCountBeforeReplication := timeSeriesCount / len(zones)
			for i := 0; i < timeSeriesCountBeforeReplication; i++ {
				token := r.Uint32()
				ringToken := tokensFromZone[searchToken(tokensFromZone, token)]
				instance, ok := shard.ringInstanceByToken[ringToken]
				require.True(t, ok)
				count, ok := timeSeriesCountByInstance[instance.InstanceID]
				if !ok {
					count = 0
				}
				count++
				timeSeriesCountByInstance[instance.InstanceID] = count
			}
			timeSeriesCountByInstanceByZone[zone] = timeSeriesCountByInstance
		}
		fmt.Printf("completed\n")
	}

	printSimulationResults(t, zones, ingestersPerZone, timeSeriesCountByInstanceByZone, tenantCountByInstance)
}

func Test_RealRing_RealShardSizes_RandomShuffleSharder(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRealRing(t, testingCell)
	require.NotNil(t, ring)
	ring.sharder = randomShuffleSharder{}
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, nil)
}

func Test_SMTRing_RealShardSizes_RandomShuffleSharder(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	ring.sharder = randomShuffleSharder{}
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, nil)
}

func Test_RealRing_Power2ShardSizes_RandomShuffleSharder(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRealRing(t, testingCell)
	require.NotNil(t, ring)
	ring.sharder = randomShuffleSharder{}
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, getPower2ShardSize)
}

func Test_SMTRing_Power2ShardSizes_RandomShuffleSharder(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	ring.sharder = randomShuffleSharder{}
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, getPower2ShardSize)
}

func Test_RealRing_RealShardSizes_MonteCarlohuffleSharder(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRealRing(t, testingCell)
	require.NotNil(t, ring)
	ring.sharder = monteCarloShuffleSharder{smtModeEnabled: false}
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, nil)
}

func Test_SMTRing_RealShardSizes_MonteCarlohuffleSharder_SMTModeDisabled(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	ring.sharder = monteCarloShuffleSharder{smtModeEnabled: false}
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, nil)
}

func Test_SMTRing_RealShardSizes_MonteCarlohuffleSharder_SMTModeEnabled(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	ring.sharder = monteCarloShuffleSharder{smtModeEnabled: true}
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, nil)
}

func Test_RealRing_Power2ShardSizes_MonteCarlohuffleSharder(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRealRing(t, testingCell)
	require.NotNil(t, ring)
	ring.sharder = monteCarloShuffleSharder{smtModeEnabled: false}
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, getPower2ShardSize)
}

func Test_SMTRing_Power2ShardSizes_MonteCarlohuffleSharder_SMTModeDisabled(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	ring.sharder = monteCarloShuffleSharder{smtModeEnabled: false}
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, getPower2ShardSize)
}

func Test_SMTRing_Power2ShardSizes_MonteCarlohuffleSharder_SMTModeEnabled(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	ring.sharder = monteCarloShuffleSharder{smtModeEnabled: true}
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, getPower2ShardSize)
}

func Test_RealRing_RealShardSizes_HRWShuffleSharder(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRealRing(t, testingCell)
	require.NotNil(t, ring)
	ring.sharder = newHRWShuffleSharder(ring.ringDesc.GetIngesters(), ring.ringZones, false)
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, nil)
}

func Test_SMTRing_RealShardSizes_HRWShuffleSharder_SMTModeDisabled(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	ring.sharder = newHRWShuffleSharder(ring.ringDesc.GetIngesters(), ring.ringZones, false)
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, nil)
}

func Test_SMTRing_RealShardSizes_HRWShuffleSharder_SMTModeEnabled(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	ring.sharder = newHRWShuffleSharder(ring.ringDesc.GetIngesters(), ring.ringZones, true)
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, nil)
}

func Test_RealRing_Power2ShardSizes_HRWShuffleSharder(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRealRing(t, testingCell)
	require.NotNil(t, ring)
	ring.sharder = newHRWShuffleSharder(ring.ringDesc.GetIngesters(), ring.ringZones, false)
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, getPower2ShardSize)
}

func Test_SMTRing_Power2ShardSizes_HRWShuffleSharder_SMTModeDisabled(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	ring.sharder = newHRWShuffleSharder(ring.ringDesc.GetIngesters(), ring.ringZones, false)
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, getPower2ShardSize)
}

func Test_SMTRing_Power2ShardSizes_HRWShuffleSharder_SMTModeEnabled(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 157
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	ring.sharder = newHRWShuffleSharder(ring.ringDesc.GetIngesters(), ring.ringZones, true)
	testTimeseriesDistribution(t, testingDir, testingCell, zones, ingestersPerZone, ring, getPower2ShardSize)
}

func getCustomizedShardSize(shardSize, numZones, ingestersPerZone int) int {
	totalIngesters := numZones * ingestersPerZone
	if shardSize == 0 || shardSize >= totalIngesters {
		return totalIngesters
	}
	acceptedShardSizes := []int{3, 9, 18, 33, 66, 129, 258, 471}
	var i int
	for i = 0; i < len(acceptedShardSizes) && acceptedShardSizes[i] < shardSize; i++ {
	}
	if i == len(acceptedShardSizes) {
		return totalIngesters
	}
	return shardUtil.ShuffleShardExpectedInstances(acceptedShardSizes[i], numZones)
}

func getPower2ShardSize(shardSize, numZones, ingestersPerZone int) int {
	totalIngesters := numZones * ingestersPerZone
	if shardSize == 0 {
		return totalIngesters
	}
	if shardSize >= totalIngesters {
		return totalIngesters
	}
	exp := int(math.Log2(float64(shardSize)))
	max := int(math.Min(math.Pow(2, float64(exp+1)), float64(totalIngesters)))
	return shardUtil.ShuffleShardExpectedInstances(max, numZones)
}

func updateTenantCountByInstance(tenantCountByInstance map[string]int, shard *Ring) {
	instances := shard.ringDesc.GetIngesters()
	for instanceID := range instances {
		tenantCount, ok := tenantCountByInstance[instanceID]
		if !ok {
			tenantCount = 0
		}
		tenantCountByInstance[instanceID] = tenantCount + 1
	}
}

func printSimulationResults(t *testing.T, zones []string, ingestersPerZone int, timeSeriesCountByInstanceByZone map[string]map[string]int, tenantsByInstance map[string]int) {
	res := make(byCount, 0, len(zones)*ingestersPerZone)

	for zone, timeSeriesCountByInstance := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %s\n", zone)
		fmt.Println(strings.Repeat("-", 30))
		min := math.MaxFloat64
		max := 0.0
		for instance, timeSeriesCount := range timeSeriesCountByInstance {
			tenantCount, ok := tenantsByInstance[instance]
			if !ok {
				tenantCount = 0
			}
			min = math.Min(min, float64(timeSeriesCount))
			max = math.Max(max, float64(timeSeriesCount))
			res = append(res, instanceWithCount{
				instanceID:      instance,
				timeseriesCount: timeSeriesCount,
				tenantCount:     tenantCount,
			})
		}
		spread := 1 - min/max
		fmt.Printf("SPREAD FOR ZONE %s is %f\n", zone, spread)
	}

	slices.SortFunc(res, func(a, b instanceWithCount) bool {
		prefixA, idA, errA := parseInstanceID(a.instanceID)
		require.NoError(t, errA)
		prefixB, idB, errB := parseInstanceID(b.instanceID)
		require.NoError(t, errB)
		if prefixA < prefixB {
			return true
		}
		if prefixA == prefixB {
			return idA < idB
		}
		return false
	})

	for _, ic := range res {
		fmt.Printf("%30s, %10d, %5d\n", ic.instanceID, ic.timeseriesCount, ic.tenantCount)
	}
}
