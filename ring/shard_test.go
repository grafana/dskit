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

func createRealRing(t *testing.T, cell string) *Ring {
	ringDesc, err := GetRealRingDesc("", cell)
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

func TestInputData(t *testing.T) {
	shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp10")
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp10")
	require.NoError(t, err)
	maxGlobalSeriesPerTenant, err := GetMaxGlobalLimitByTenantID("", "cp10")
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
			timeseriesCount: currTimeseriesCount,
			maxGlobalLimit:  maxGlobalLimit,
		})
	}

	sort.Sort(data)
	for _, tI := range data {
		fmt.Printf("%10s %10d %10d %10d\n", tI.tenantID, tI.timeseriesCount, tI.maxGlobalLimit, tI.shardSize)
	}
}

func TestInputData1(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 168
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp10")
	require.NoError(t, err)
	//timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp10")
	//timeSeriesCountByTenantID, err := GetMaxGlobalLimitByTenantID("", "cp10")

	slices.Sort(zones)

	stat := make(map[string]map[int]int, ingestersPerZone)

	for tenantID, shardSize := range shardSizeByTenantID {
		random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(tenantID, "")))
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

func TestSpreadMinimizingRingNormalShard(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 168
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	ring.sharder = newSpreadMinimizingShuffleSharder()
	require.NotNil(t, ring)
	shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp10")
	require.NoError(t, err)
	//timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp10")
	timeSeriesCountByTenantID, err := GetMaxGlobalLimitByTenantID("", "cp10")
	require.NoError(t, err)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	timeSeriesCountByInstanceByZone := make(map[string]map[string]int, len(ring.ringZones))
	tenantCount := len(timeSeriesCountByTenantID)
	currCount := 0
	for tenantId, timeSeriesCount := range timeSeriesCountByTenantID {
		currCount++
		fmt.Printf("handling tenant: %s (%d out of %d)... ", tenantId, currCount, tenantCount)
		shardSize, ok := shardSizeByTenantID[tenantId]
		if !ok {
			shardSize = 0
		}
		shard, ok := ring.ShuffleShard(tenantId, shardSize).(*Ring)
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

	for zone, timeSeriesCountByInstance := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %s\n", zone)
		fmt.Println(strings.Repeat("-", 30))
		min := math.MaxFloat64
		max := 0.0
		for instance, timeSeriesCount := range timeSeriesCountByInstance {
			fmt.Printf("instance: %30s, timeSeriesCount: %10d\n", instance, timeSeriesCount)
			min = math.Min(min, float64(timeSeriesCount))
			max = math.Max(max, float64(timeSeriesCount))
		}
		spread := 1 - min/max
		fmt.Printf("SPREAD FOR ZONE %s is %f\n", zone, spread)
	}
}

func TestSpreadMinimizingShuffleShardSpread(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 168
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	ring.sharder = newSpreadMinimizingShuffleSharder()
	require.NotNil(t, ring)
	//shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp10")
	//require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp10")
	//timeSeriesCountByTenantID, err := GetMaxGlobalLimitByTenantID("", "cp10")
	require.NoError(t, err)
	//tenantCount := len(timeSeriesCountByTenantID)
	//currCount := 0
	for tenantId, _ := range timeSeriesCountByTenantID {
		for shardSize := 0; shardSize < 3*ingestersPerZone; shardSize = shardSize + 3 {
			fmt.Printf("handling tenant: %s with shard size %d... ", tenantId, shardSize)
			_, ok := ring.ShuffleShard(tenantId, shardSize).(*Ring)
			require.True(t, ok)
			fmt.Printf("completed\n")
		}
	}
	/*for tenantId, _ := range timeSeriesCountByTenantID {
		currCount++
		shardSize, ok := shardSizeByTenantID[tenantId]
		if !ok {
			shardSize = 0
		}
		fmt.Printf("handling tenant: %s with shard size %d (%d out of %d)... ", tenantId, shardSize, currCount, tenantCount)
		_, ok = ring.ShuffleShard(tenantId, shardSize).(*Ring)
		require.True(t, ok)
		fmt.Printf("completed\n")
	}*/
}

/*func TestSpreadMinimizingTokensRingHRWShard(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	const ingestersPerZone = 168
	ring := createRingWithSpreadMinimizingTokens(t, zones, ingestersPerZone)
	require.NotNil(t, ring)
	shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp10")
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp10")
	require.NoError(t, err)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	timeSeriesCountByInstanceByZone := make(map[string]map[string]int, len(ring.ringZones))
	for tenantId, timeSeriesCount := range timeSeriesCountByTenantID {
		//fmt.Printf("handling tenant: %s... ", tenantId)
		shardSize, ok := shardSizeByTenantID[tenantId]
		if !ok {
			shardSize = 0
		}
		shard := ring.HRWShuffleShard(tenantId, shardSize)
		for _, zone := range ring.ringZones {
			tokensFromZone := shard.ringTokensByZone[zone]
			timeSeriesCountByInstance, ok := timeSeriesCountByInstanceByZone[zone]
			if !ok {
				timeSeriesCountByInstance = make(map[string]int, len(ring.ringDesc.Ingesters))
			}
			for i := 0; i < timeSeriesCount; i++ {
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
		//fmt.Printf("completed\n")
	}

	minByZone := make(map[string]float64, len(ring.ringZones))
	maxByZone := make(map[string]float64, len(ring.ringZones))
	for zone, timeSeriesCountByInstance := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %s\n", zone)
		fmt.Println(strings.Repeat("-", 30))
		minByZone[zone] = math.MaxFloat64
		maxByZone[zone] = 0.0
		for instance, timeSeriesCount := range timeSeriesCountByInstance {
			fmt.Printf("instance: %30s, timeSeriesCount: %10d\n", instance, timeSeriesCount)
			minByZone[zone] = math.Min(minByZone[zone], float64(timeSeriesCount))
			maxByZone[zone] = math.Max(maxByZone[zone], float64(timeSeriesCount))
		}
		spread := 1 - minByZone[zone]/maxByZone[zone]
		fmt.Printf("SPREAD FOR ZONE %s is %f\n", zone, spread)
	}

	for zone := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %10s, MIN: %10.3f, MAX: %10.3f\n", zone, minByZone[zone], maxByZone[zone])
	}
}*/

func TestRealRingNormalShard(t *testing.T) {
	ring := createRealRing(t, "cp10")
	shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp10")
	require.NoError(t, err)
	//timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp10")
	timeSeriesCountByTenantID, err := GetMaxGlobalLimitByTenantID("", "cp10")
	require.NoError(t, err)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	timeSeriesCountByInstanceByZone := make(map[string]map[string]int, len(ring.ringZones))
	tenantCount := len(timeSeriesCountByTenantID)
	currCount := 0
	for tenantId, timeSeriesCount := range timeSeriesCountByTenantID {
		currCount++
		fmt.Printf("handling tenant: %s (%d out of %d)... ", tenantId, currCount, tenantCount)
		shardSize, ok := shardSizeByTenantID[tenantId]
		if !ok {
			shardSize = 0
		}
		shard, ok := ring.ShuffleShard(tenantId, shardSize).(*Ring)
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

	for zone, timeSeriesCountByInstance := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %s\n", zone)
		fmt.Println(strings.Repeat("-", 30))
		min := math.MaxFloat64
		max := 0.0
		for instance, timeSeriesCount := range timeSeriesCountByInstance {
			fmt.Printf("instance: %30s, timeSeriesCount: %10d\n", instance, timeSeriesCount)
			min = math.Min(min, float64(timeSeriesCount))
			max = math.Max(max, float64(timeSeriesCount))
		}
		spread := 1 - min/max
		fmt.Printf("SPREAD FOR ZONE %s is %f\n", zone, spread)
	}
}

/*func TestSort(t *testing.T) {
	hashes := make([]hashedInstanceID, 0, 3)
	hashes = append(hashes, hashedInstanceID{
		hash:       10,
		instanceID: "C",
	})
	hashes = append(hashes, hashedInstanceID{
		hash:       9,
		instanceID: "B",
	})
	hashes = append(hashes, hashedInstanceID{
		hash:       10,
		instanceID: "A",
	})
	slices.SortFunc(hashes, func(a, b hashedInstanceID) bool {
		return a.hash < b.hash || (a.hash == b.hash && a.instanceID < b.instanceID)
	})
	fmt.Println(hashes)
	require.Len(t, hashes, 3)
}*/

/*func TestRealRingHRWShard(t *testing.T) {
	ring := createRealRing(t, "cp10")
	shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp10")
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp10")
	require.NoError(t, err)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	timeSeriesCountByInstanceByZone := make(map[string]map[string]int, len(ring.ringZones))
	for tenantId, timeSeriesCount := range timeSeriesCountByTenantID {
		//fmt.Printf("handling tenant: %s... ", tenantId)
		shardSize, ok := shardSizeByTenantID[tenantId]
		if !ok {
			shardSize = 0
		}
		shard := ring.HRWShuffleShard(tenantId, shardSize)
		for _, zone := range ring.ringZones {
			tokensFromZone := shard.ringTokensByZone[zone]
			timeSeriesCountByInstance, ok := timeSeriesCountByInstanceByZone[zone]
			if !ok {
				timeSeriesCountByInstance = make(map[string]int, len(ring.ringDesc.Ingesters))
			}
			for i := 0; i < timeSeriesCount; i++ {
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
		//fmt.Printf("completed\n")
	}

	minByZone := make(map[string]float64, len(ring.ringZones))
	maxByZone := make(map[string]float64, len(ring.ringZones))
	for zone, timeSeriesCountByInstance := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %s\n", zone)
		fmt.Println(strings.Repeat("-", 30))
		minByZone[zone] = math.MaxFloat64
		maxByZone[zone] = 0.0
		for instance, timeSeriesCount := range timeSeriesCountByInstance {
			fmt.Printf("instance: %30s, timeSeriesCount: %10d\n", instance, timeSeriesCount)
			minByZone[zone] = math.Min(minByZone[zone], float64(timeSeriesCount))
			maxByZone[zone] = math.Max(maxByZone[zone], float64(timeSeriesCount))
		}
		spread := 1 - minByZone[zone]/maxByZone[zone]
		fmt.Printf("SPREAD FOR ZONE %s is %f\n", zone, spread)
	}

	for zone := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %10s, MIN: %10.3f, MAX: %10.3f\n", zone, minByZone[zone], maxByZone[zone])
	}
}*/

func TestCP01RealRingNormalShard(t *testing.T) {
	ring := createRealRing(t, "cp01")
	shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp01")
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp01")
	require.NoError(t, err)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	timeSeriesCountByInstanceByZone := make(map[string]map[string]int, len(ring.ringZones))
	for tenantId, timeSeriesCount := range timeSeriesCountByTenantID {
		//fmt.Printf("handling tenant: %s... ", tenantId)
		shardSize, ok := shardSizeByTenantID[tenantId]
		if !ok {
			shardSize = 0
		}
		shard, ok := ring.ShuffleShard(tenantId, shardSize).(*Ring)
		for _, zone := range ring.ringZones {
			tokensFromZone := shard.ringTokensByZone[zone]
			timeSeriesCountByInstance, ok := timeSeriesCountByInstanceByZone[zone]
			if !ok {
				timeSeriesCountByInstance = make(map[string]int, len(ring.ringDesc.Ingesters))
			}
			for i := 0; i < timeSeriesCount; i++ {
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
		//fmt.Printf("completed\n")
	}

	minByZone := make(map[string]float64, len(ring.ringZones))
	maxByZone := make(map[string]float64, len(ring.ringZones))
	for zone, timeSeriesCountByInstance := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %s\n", zone)
		fmt.Println(strings.Repeat("-", 30))
		minByZone[zone] = math.MaxFloat64
		maxByZone[zone] = 0.0
		for instance, timeSeriesCount := range timeSeriesCountByInstance {
			fmt.Printf("instance: %30s, timeSeriesCount: %10d\n", instance, timeSeriesCount)
			minByZone[zone] = math.Min(minByZone[zone], float64(timeSeriesCount))
			maxByZone[zone] = math.Max(maxByZone[zone], float64(timeSeriesCount))
		}
		spread := 1 - minByZone[zone]/maxByZone[zone]
		fmt.Printf("SPREAD FOR ZONE %s is %f\n", zone, spread)
	}
	for zone := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %10s, MIN: %10.3f, MAX: %10.3f\n", zone, minByZone[zone], maxByZone[zone])
	}
}

func TestCP01ShardAnalysis(t *testing.T) {
	ring := createRealRing(t, "cp01")
	shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp01")
	require.NoError(t, err)
	//timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp01")
	//require.NoError(t, err)
	maxGlobalSeriesByTenantID, err := GetMaxGlobalLimitByTenantID("", "cp01")
	require.NoError(t, err)
	tenantsByInstanceByZone := getTenantsByInstanceByZone(shardSizeByTenantID, ring)
	for zone, tenantsByInstance := range tenantsByInstanceByZone {
		fmt.Printf("ZONE: %s\n", zone)
		fmt.Println(strings.Repeat("-", 30))
		for instance, tenants := range tenantsByInstance {
			maxSeries := 0.0
			for _, tenant := range tenants {
				maxGlobalSeries, ok := maxGlobalSeriesByTenantID[tenant]
				if !ok {
					maxGlobalSeries = 0
				}
				maxSeries += float64(maxGlobalSeries)
			}
			avg := maxSeries / float64(len(tenants))
			fmt.Printf("instance: %30s, #tenants: (%5d), avgMaxSeriesPerTenant: (%10.3f), tenants: %v\n", instance, len(tenants), avg, tenants)
		}
		fmt.Println()
	}
}

func getTenantsByInstanceByZone(shardSizeByTenantID map[string]int, ring *Ring) map[string]map[string][]string {
	tenantsByInstanceByZone := make(map[string]map[string][]string, len(ring.ringZones))
	for tenantId := range shardSizeByTenantID {
		//fmt.Printf("handling tenant: %s... ", tenantId)
		shardSize, ok := shardSizeByTenantID[tenantId]
		if !ok {
			shardSize = 0
		}
		shard, ok := ring.ShuffleShard(tenantId, shardSize).(*Ring)
		instances := shard.ringDesc.GetIngesters()
		for _, instance := range instances {
			zone := instance.GetZone()
			tenantsByInstance, ok := tenantsByInstanceByZone[zone]
			if !ok {
				tenantsByInstance = make(map[string][]string, len(ring.ringDesc.Ingesters)/len(shard.ringZones))
			}
			tenants, ok := tenantsByInstance[instance.GetId()]
			if !ok {
				tenants = make([]string, 0, len(shardSizeByTenantID))
			}
			tenants = append(tenants, tenantId)
			tenantsByInstance[instance.GetId()] = tenants
			tenantsByInstanceByZone[zone] = tenantsByInstance
		}
	}
	return tenantsByInstanceByZone
}

func getTenantsByInstance(tenantsByInstanceByZone map[string]map[string][]string) map[string][]string {
	result := make(map[string][]string)
	for _, tenantsByInstance := range tenantsByInstanceByZone {
		for instance, tenants := range tenantsByInstance {
			result[instance] = tenants
		}
	}
	return result
}

func TestCP01IncreasingShardMaxSeriesAnalysis(t *testing.T) {
	ring := createRealRing(t, "cp01")
	shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp01")
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp01")
	require.NoError(t, err)
	tenantsByInstanceByZone := getTenantsByInstanceByZone(shardSizeByTenantID, ring)
	tenantsByInstance := getTenantsByInstance(tenantsByInstanceByZone)
	/*maxInstancesByZone := map[string][]string{
		"zone-a": {
			"ingester-zone-a-2",
			"ingester-zone-a-57",
			"ingester-zone-a-80",
		},
		"zone-b": {
			"ingester-zone-b-37",
			"ingester-zone-b-42",
			"ingester-zone-b-112",
		},
		"zone-c": {
			"ingester-zone-c-52",
			"ingester-zone-c-57",
			"ingester-zone-c-63",
			"ingester-zone-c-105",
		},
	}*/
	maxInstances := []string{
		"ingester-zone-a-2",
		"ingester-zone-a-57",
		"ingester-zone-a-80",
		"ingester-zone-b-37",
		"ingester-zone-b-42",
		"ingester-zone-b-112",
		"ingester-zone-c-52",
		"ingester-zone-c-57",
		"ingester-zone-c-63",
		"ingester-zone-c-105",
	}

	r := rand.New(rand.NewSource(time.Now().Unix()))
	timeSeriesCountByInstanceByZone := make(map[string]map[string]int, len(ring.ringZones))
	for tenantId, timeSeriesCount := range timeSeriesCountByTenantID {
		shardSize, ok := shardSizeByTenantID[tenantId]
		if !ok {
			shardSize = 0
		}
		shard, ok := ring.ShuffleShard(tenantId, shardSize).(*Ring)
		require.True(t, ok)

		if shardSize >= 50 {
			modifiedShardsByTenantID := make(map[string]bool, len(shardSizeByTenantID))

			for _, instance := range maxInstances {
				_, ok := modifiedShardsByTenantID[tenantId]
				if !ok {
					tenants := tenantsByInstance[instance]
					newShardSize := int(float64(shardSize) * 1.2)
					if newShardSize > shardSize && slices.Contains(tenants, tenantId) {
						fmt.Printf("\t\tshard size of tenantID %s changed from %d to %d\n", tenantId, shardSize, newShardSize)
						shard, ok = ring.ShuffleShard(tenantId, newShardSize).(*Ring)
						require.True(t, ok)
						modifiedShardsByTenantID[tenantId] = true
					}
				}
			}
		}

		for _, zone := range ring.ringZones {
			tokensFromZone := shard.ringTokensByZone[zone]
			timeSeriesCountByInstance, ok := timeSeriesCountByInstanceByZone[zone]
			if !ok {
				timeSeriesCountByInstance = make(map[string]int, len(ring.ringDesc.Ingesters))
			}
			for i := 0; i < timeSeriesCount; i++ {
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
	}

	minByZone := make(map[string]float64, len(ring.ringZones))
	maxByZone := make(map[string]float64, len(ring.ringZones))
	for zone, timeSeriesCountByInstance := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %s\n", zone)
		fmt.Println(strings.Repeat("-", 30))
		minByZone[zone] = math.MaxFloat64
		maxByZone[zone] = 0.0
		for instance, timeSeriesCount := range timeSeriesCountByInstance {
			fmt.Printf("instance: %30s, timeSeriesCount: %10d\n", instance, timeSeriesCount)
			minByZone[zone] = math.Min(minByZone[zone], float64(timeSeriesCount))
			maxByZone[zone] = math.Max(maxByZone[zone], float64(timeSeriesCount))
		}
		spread := 1 - minByZone[zone]/maxByZone[zone]
		fmt.Printf("SPREAD FOR ZONE %s is %f\n", zone, spread)
	}
	for zone := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %10s, MIN: %10.3f, MAX: %10.3f\n", zone, minByZone[zone], maxByZone[zone])
	}
}

func TestCP01IncreasingShardAnalysis(t *testing.T) {
	ring := createRealRing(t, "cp01")
	shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp01")
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp01")
	require.NoError(t, err)
	require.NoError(t, err)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	timeSeriesCountByInstanceByZone := make(map[string]map[string]int, len(ring.ringZones))
	for tenantId, timeSeriesCount := range timeSeriesCountByTenantID {
		//fmt.Printf("handling tenant: %s... ", tenantId)
		shardSize, ok := shardSizeByTenantID[tenantId]
		if !ok {
			shardSize = 0
		}
		shard, ok := ring.ShuffleShard(tenantId, shardSize).(*Ring)
		require.True(t, ok)
		extendedShard, ok := ring.ShuffleShard(tenantId, (shardSize + 3)).(*Ring)
		require.True(t, ok)
		maxs := []string{
			"ingester-zone-a-32",
			"ingester-zone-a-71",
			"ingester-zone-a-74",
			"ingester-zone-a-100",
			"ingester-zone-b-0",
			"ingester-zone-b-18",
			"ingester-zone-b-47",
			"ingester-zone-b-49",
			"ingester-zone-b-52",
			"ingester-zone-b-58",
			"ingester-zone-b-77",
			"ingester-zone-c-3",
			"ingester-zone-c-10",
			"ingester-zone-c-18",
			"ingester-zone-c-30",
			"ingester-zone-c-38",
			"ingester-zone-c-43",
			"ingester-zone-c-49",
			"ingester-zone-c-67",
		}
		if isShardOfInterest(shard, extendedShard, maxs) {
			fmt.Printf("\t\tshard size of tenantID %s changed from %d to %d\n", tenantId, shardSize, len(extendedShard.ringDesc.Ingesters))
			shard = extendedShard
		}
		for _, zone := range ring.ringZones {
			tokensFromZone := shard.ringTokensByZone[zone]
			timeSeriesCountByInstance, ok := timeSeriesCountByInstanceByZone[zone]
			if !ok {
				timeSeriesCountByInstance = make(map[string]int, len(ring.ringDesc.Ingesters))
			}
			for i := 0; i < timeSeriesCount; i++ {
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
		//fmt.Printf("completed\n")
	}

	minByZone := make(map[string]float64, len(ring.ringZones))
	maxByZone := make(map[string]float64, len(ring.ringZones))
	for zone, timeSeriesCountByInstance := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %s\n", zone)
		fmt.Println(strings.Repeat("-", 30))
		minByZone[zone] = math.MaxFloat64
		maxByZone[zone] = 0.0
		for instance, timeSeriesCount := range timeSeriesCountByInstance {
			fmt.Printf("instance: %30s, timeSeriesCount: %10d\n", instance, timeSeriesCount)
			minByZone[zone] = math.Min(minByZone[zone], float64(timeSeriesCount))
			maxByZone[zone] = math.Max(maxByZone[zone], float64(timeSeriesCount))
		}
		spread := 1 - minByZone[zone]/maxByZone[zone]
		fmt.Printf("SPREAD FOR ZONE %s is %f\n", zone, spread)
	}
	for zone := range timeSeriesCountByInstanceByZone {
		fmt.Printf("ZONE: %10s, MIN: %10.3f, MAX: %10.3f\n", zone, minByZone[zone], maxByZone[zone])
	}
}

func isShardOfInterest(shard, extendedShard *Ring, instancesOfInterest []string) bool {
	instances := shard.ringDesc.GetIngesters()
	extendedInstances := extendedShard.ringDesc.GetIngesters()
	for instance := range extendedInstances {
		if _, ok := instances[instance]; !ok {
			if slices.Contains(instancesOfInterest, instance) {
				return true
			}
		}
	}
	return false
}

func TestShard_Compare(t *testing.T) {
	shardSizeByTenantID, err := GetShardSizeByTenantID("", "cp01")
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID("", "cp01")
	require.NoError(t, err)
	maxGlobalSeriesByTenantID, err := GetMaxGlobalLimitByTenantID("", "cp01")
	require.NoError(t, err)
	totalShard := 0
	for tenantID := range shardSizeByTenantID {
		_, ok := timeSeriesCountByTenantID[tenantID]
		/*if !ok {
			fmt.Printf("%s (%d) not present in timeseriesCount\n", tenantID, shardSizeByTenantID[tenantID])
		}*/
		require.True(t, ok)
		shardSize := shardSizeByTenantID[tenantID]
		totalShard += shardSize
	}
	total := 0
	for tenantID := range timeSeriesCountByTenantID {
		_, ok := shardSizeByTenantID[tenantID]
		/*if !ok {
			fmt.Printf("%s (%d) not present in shardSize\n", tenantID, timeSeriesCountByTenantID[tenantID])
		}*/
		require.True(t, ok)
		total += timeSeriesCountByTenantID[tenantID]
	}
	fmt.Printf("Total number of series per zone: %d, number of series (all zones): %d, total ingesters in shards (all zones): %d\n", total, total*3, totalShard)

	type stat struct {
		tenantID             string
		shardSize            int
		timeseriesCount      int
		maxGlobalSeriesLimit int
		ratio                float64
	}

	statistics := make([]stat, 0, len(shardSizeByTenantID))
	for tenantID := range shardSizeByTenantID {
		s := stat{
			tenantID:             tenantID,
			shardSize:            shardSizeByTenantID[tenantID],
			timeseriesCount:      timeSeriesCountByTenantID[tenantID],
			maxGlobalSeriesLimit: maxGlobalSeriesByTenantID[tenantID],
			ratio:                float64(timeSeriesCountByTenantID[tenantID]) / float64(shardSizeByTenantID[tenantID]),
		}
		statistics = append(statistics, s)
	}

	slices.SortFunc(statistics, func(a, b stat) bool {
		return a.ratio < b.ratio
	})

	for _, st := range statistics {
		fmt.Printf("tenantId: %10s, shardSize: %3d, timeseriesCount: %10d, maxGlobalSeriesPerUser: %10d, ration: %15.3f\n", st.tenantID, st.shardSize, st.timeseriesCount, st.maxGlobalSeriesLimit, st.ratio)
	}
}
