package ring

import (
	"fmt"
	"hash/fnv"
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
	testingCell = "cortex-prod-13"
	testingDir  = ""
)

func createRealRingWithIngesterReplicas(t *testing.T, cell string, ingesterReplicasPerZone int, zones []string) *Ring {
	ringDesc, err := GetRealRingDescWithIngesterReplicas(testingDir, cell, ingesterReplicasPerZone, zones)
	require.NoError(t, err)
	r := &Ring{
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
	r.setRingStateFromDesc(ringDesc, false, false, false)
	return r
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
			ringDesc.AddIngester(instanceID, instanceID, zone, tokens, ACTIVE, time.Now(), false, time.Now())
		}
	}
	r := &Ring{
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
	r.setRingStateFromDesc(ringDesc, false, false, false)
	return r
}

type instanceWithCount struct {
	instanceID      string
	timeseriesCount int
	tenantCount     int
}

type byCount []instanceWithCount

func (c byCount) Len() int      { return len(c) }
func (c byCount) Swap(i, j int) { c[i], c[j] = c[j], c[i] }
func (c byCount) Less(i, j int) bool {
	if c[i].timeseriesCount > c[j].timeseriesCount {
		return true
	}
	if c[i].timeseriesCount == c[j].timeseriesCount {
		return c[i].instanceID < c[j].instanceID
	}
	return false
}

type tenantInfo struct {
	tenantID        string
	timeseriesCount int
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

func TestInputDataByTenant(t *testing.T) {
	shardSizeByTenantID, err := GetShardSizeByTenantID(testingDir, testingCell)
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID(testingDir, testingCell)
	require.NoError(t, err)
	data := make(byTenantInfo, 0, len(shardSizeByTenantID))
	for tenantID, shardSize := range shardSizeByTenantID {
		currTimeseriesCount, ok := timeSeriesCountByTenantID[tenantID]
		if !ok {
			currTimeseriesCount = 0
		}
		data = append(data, tenantInfo{
			tenantID:        tenantID,
			shardSize:       shardSize,
			timeseriesCount: currTimeseriesCount / 3,
		})
	}

	sort.Sort(data)
	for _, tI := range data {
		fmt.Printf("%10s %10d %10d\n", tI.tenantID, tI.timeseriesCount, tI.shardSize)
	}
}

func testTimeseriesDistribution(t *testing.T, dir string, cell string, zones []string, ingestersPerZone int, ring *Ring, timeseriesAlreadyReplicated bool, shardUpdater func(int, int, int) int, timeseriesTimestamp *string, excludeTenant func(int) bool) {
	var (
		bufDescs [GetBufferSize]InstanceDesc
		bufHosts [GetBufferSize]string
		bufZones [GetBufferSize]string
	)
	shardSizeByTenantID, err := GetShardSizeByTenantID(dir, cell)
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID(dir, cell)
	//timeSeriesCountByTenantID, err := GetMaxGlobalLimitByTenantID(dir, cell)
	require.NoError(t, err)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	timeSeriesCountByInstanceByZone := make(map[string]map[string]int, len(ring.ringZones))
	tenantCountByInstance := make(map[string]int, len(zones)*ingestersPerZone)
	tenantsByInstance := make(map[string]map[string]struct{}, len(zones)*ingestersPerZone)
	tenantCount := len(timeSeriesCountByTenantID)
	currCount := 0
	percentage := 10
	for tenantId, timeSeriesCount := range timeSeriesCountByTenantID {
		currCount++
		if currCount*100/tenantCount >= percentage {
			status := fmt.Sprintf("%s - %d%%", cell, percentage)
			fmt.Println(status)
			percentage += 10
		}
		//fmt.Printf("handling tenant: %s (%d out of %d)... ", tenantId, currCount, tenantCount)
		shardSize, ok := shardSizeByTenantID[tenantId]
		if !ok {
			shardSize = 0
		}
		if shardUpdater != nil {
			shardSize = shardUpdater(shardSize, len(zones), ingestersPerZone)
		}
		if excludeTenant(shardSize) {
			continue
		}
		shard, ok := ring.ShuffleShard(tenantId, shardSize).(*Ring)

		if timeseriesTimestamp != nil {
			seed := timeseriesSeed(tenantId, *timeseriesTimestamp)
			r = rand.New(rand.NewSource(seed))
		}

		timeSeriesCountBeforeReplication := timeSeriesCount
		if timeseriesAlreadyReplicated {
			timeSeriesCountBeforeReplication /= len(zones)
		}
		for i := 0; i < timeSeriesCountBeforeReplication; i++ {
			token := r.Uint32()
			rs, err := shard.Get(token, WriteNoExtend, bufDescs[:0], bufHosts[:0], bufZones[:0])
			require.NoError(t, err)
			for _, instance := range rs.Instances {
				timeSeriesCountByInstance, ok := timeSeriesCountByInstanceByZone[instance.Zone]
				if !ok {
					timeSeriesCountByInstance = make(map[string]int, len(ring.ringDesc.Ingesters))
				}
				timeSeriesCountByInstance[instance.Id] = timeSeriesCountByInstance[instance.Id] + 1
				timeSeriesCountByInstanceByZone[instance.Zone] = timeSeriesCountByInstance
				tenants, ok := tenantsByInstance[instance.Id]
				if !ok {
					tenants = make(map[string]struct{}, len(tenantsByInstance))
				}
				tenants[tenantId] = struct{}{}
				tenantsByInstance[instance.Id] = tenants
			}
		}
	}
	fmt.Printf("\n%s completed\n", cell)
	for instance, tenants := range tenantsByInstance {
		tenantCountByInstance[instance] = len(tenants)
	}
	printSimulationResults(t, zones, ingestersPerZone, timeSeriesCountByInstanceByZone, tenantCountByInstance)
}

func printSimulationResults(t *testing.T, zones []string, ingestersPerZone int, timeSeriesCountByInstanceByZone map[string]map[string]int, tenantsByInstance map[string]int) {
	res := make(byCount, 0, len(zones)*ingestersPerZone)

	totalMin := math.MaxFloat64
	totalMax := 0.0

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
			totalMin = math.Min(totalMin, float64(timeSeriesCount))
			totalMax = math.Max(totalMax, float64(timeSeriesCount))
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

	fmt.Printf("Ingester with minimum timeseries has %d timeseries.\n", int(totalMin))
	fmt.Printf("Ingester with maximum timeseries has %d timeseries.\n", int(totalMax))

	for _, ic := range res {
		fmt.Printf("%30s, %10d, %5d\n", ic.instanceID, ic.timeseriesCount, ic.tenantCount)
	}
}

func printSortedTenantsCountByShardSize(shardSizesByTenant map[string]int, zones []string) {
	sizes := make(map[int]int, 100)
	for _, shardSize := range shardSizesByTenant {
		s := shardUtil.ShuffleShardExpectedInstances(shardSize, len(zones))
		sizes[s] = sizes[s] + 1
	}
	allSizes := make([]int, 0, len(sizes))
	for size := range sizes {
		allSizes = append(allSizes, size)
	}
	slices.Sort(allSizes)
	for _, size := range allSizes {
		fmt.Printf("shard size: %5d, number of tenants: %10d\n", size, sizes[size])
	}
}

func Test_CP13_RealRing(t *testing.T) {
	cell := "cortex-prod-13"
	timestamp := "2024-12-18 16:30:00 +0100 CET"
	zones := []string{"zone-a", "zone-b", "zone-c"}
	ingestersPerZone := 200
	ring := createRealRingWithIngesterReplicas(t, "cortex-prod-13", ingestersPerZone, zones)
	require.NotNil(t, ring)

	exclude := func(shardSize int) bool {
		return shardSize >= ingestersPerZone*len(zones)
	}
	testTimeseriesDistribution(t, testingDir, cell, zones, ingestersPerZone, ring, false, nil, &timestamp, exclude)
}

func Test_CP13_SMT(t *testing.T) {
	cell := "cortex-prod-13"
	timestamp := "2024-12-18 16:30:00 +0100 CET"
	zones := []string{"zone-a", "zone-b", "zone-c"}
	ingestersPerZone := 100
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)

	exclude := func(shardSize int) bool {
		return shardSize >= 600
	}

	testTimeseriesDistribution(t, testingDir, cell, zones, ingestersPerZone, ring, false, nil, &timestamp, exclude)
}

func Test_CP13_RealRing_Print(t *testing.T) {
	cell := "cortex-prod-13"
	zones := []string{"zone-a", "zone-b", "zone-c"}
	ingestersPerZone := 282
	ring := createRealRingWithIngesterReplicas(t, "cortex-prod-13", ingestersPerZone, zones)
	require.NotNil(t, ring)

	shardSizeByTenantID, err := GetShardSizeByTenantID(testingDir, cell)
	require.NoError(t, err)
	printSortedTenantsCountByShardSize(shardSizeByTenantID, zones)

	timeseriesCountByTenantID, err := GetTimeseriesCountByTenantID(testingDir, cell)
	require.NoError(t, err)

	ingesterPerZoneValues := []int{200}
	currentTargets := []int{3_000_000}
	targets := []int{2_000_000, 3_000_000, 4_000_000}
	bigTenantsTimeseries := make([]int, len(ingesterPerZoneValues))
	smallTenantsTimeseries := make([]int, len(ingesterPerZoneValues))
	for i, ingestersPerZone := range ingesterPerZoneValues {
		for tenantID, shardSize := range shardSizeByTenantID {
			if shardSize > ingestersPerZone*len(zones) {
				bigTenantsTimeseries[i] += timeseriesCountByTenantID[tenantID]
			} else {
				smallTenantsTimeseries[i] += timeseriesCountByTenantID[tenantID]
			}
		}
	}

	for i, ingestersPerZone := range ingesterPerZoneValues {
		fmt.Printf("ingesters per zone: %5d (current target %dM)\n", ingestersPerZone, currentTargets[i]/1_000_000)
		fmt.Printf("\ttotal big tenants timeseries  : %10d\n", bigTenantsTimeseries[i])

		for _, target := range targets {
			fmt.Printf("\t\twould require %3d ingesters per zone with target %dM\n", bigTenantsTimeseries[i]/target, target/1_000_000)
		}

		fmt.Printf("\ttotal small tenants timeseries: %10d\n", smallTenantsTimeseries[i])

		for _, target := range targets {
			fmt.Printf("\t\twould require %3d ingesters per zone with target %dM\n", smallTenantsTimeseries[i]/target, target/1_000_000)
		}
	}
}

func timeseriesSeed(tenantID, timestamp string) int64 {
	h := fnv.New64()
	h.Write([]byte(tenantID))
	h.Write([]byte(timestamp))
	return int64(h.Sum64())
}
