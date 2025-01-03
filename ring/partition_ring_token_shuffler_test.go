package ring

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"

	shard2 "github.com/grafana/dskit/ring/shard"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func Test_SimpleRing(t *testing.T) {
	activePartitionsCount := 10
	tenantIDs := []string{"tenant-1", "tenant-2", "tenant-3", "tenant-4", "tenant-5", "tenant-6"}
	shardSizes := []int{3, 3, 3, 10, 10, 30}
	timeseriesCounts := []int{150_000, 150_000, 150_000, 1_000_000, 1_000_000, 3_000_000}

	ring := preparePartitionRingWithActivePartitions(activePartitionsCount)
	shuffler := preserveConsistencyPartitionRingTokenShuffler{}

	for i := 0; i < 10; i++ {
		timeseriesOwnershipByToken, _ := simulateDistribution(t, ring, tenantIDs, shardSizes, timeseriesCounts)
		ownershipByPartitionID := calculateOwnershipByPartitionID(ring, timeseriesOwnershipByToken)
		printSimulation(t, fmt.Sprintf("simulation number %d", i+1), ownershipByPartitionID, false)

		newRing := shuffler.shuffle(*ring, timeseriesOwnershipByToken, false)
		require.NotNil(t, newRing)

		ownershipByPartitionID = calculateOwnershipByPartitionID(newRing, timeseriesOwnershipByToken)
		printSimulation(t, fmt.Sprintf("spread after shuffling number %d", i+1), ownershipByPartitionID, false)

		compareAllShards(t, ring, newRing, tenantIDs, shardSizes)
	}
}

func Test_CortexProd13(t *testing.T) {
	activePartitionsCount := 282
	ring := preparePartitionRingWithActivePartitions(activePartitionsCount)
	shuffler := preserveConsistencyPartitionRingTokenShuffler{}

	for i := 0; i < 3; i++ {
		timeseriesOwnershipByToken := simulateTimeseriesDistribution(t, "", "cortex-prod-13", zones, ring, false)
		ownershipByPartitionID := calculateOwnershipByPartitionID(ring, timeseriesOwnershipByToken)
		printSimulation(t, fmt.Sprintf("simulation number %d", i+1), ownershipByPartitionID, false)

		newRing := ring
		for j := 0; j < 20; j++ {
			shuffledRing := shuffler.shuffle(*newRing, timeseriesOwnershipByToken, false)
			require.NotNil(t, shuffledRing)

			ownershipByPartitionID = calculateOwnershipByPartitionID(shuffledRing, timeseriesOwnershipByToken)
			printSimulation(t, fmt.Sprintf("spread after shuffling number %d-%d", i+1, j+1), ownershipByPartitionID, false)

			newRing = shuffledRing
		}
	}
}

func simulateDistribution(t *testing.T, ring *PartitionRing, tenantIDs []string, shardSizes []int, timeseriesCounts []int) (map[Token]float64, map[int32]float64) {
	timeseriesOwnershipByToken := make(map[Token]float64)
	ownershipByPartitionID := make(map[int32]float64, len(ring.Partitions()))

	for i := range tenantIDs {
		tenantID := tenantIDs[i]
		shardSize := shardSizes[i]
		timeseriesCount := timeseriesCounts[i]
		shard, err := ring.ShuffleShard(tenantID, shardSize)
		require.NoError(t, err)

		for j := 0; j < timeseriesCount; j++ {
			key := rand.Uint32()
			partitionID, closestToken, err := shard.ActivePartitionForKey(key)
			require.NoError(t, err)

			ownershipByPartitionID[partitionID] = ownershipByPartitionID[partitionID] + 1
			timeseriesOwnershipByToken[closestToken] = timeseriesOwnershipByToken[closestToken] + 1
		}
	}

	verification := make(map[int32]float64, len(ring.Partitions()))

	for token, ownership := range timeseriesOwnershipByToken {
		partitionID := ring.partitionByToken[token]
		verification[partitionID] = verification[partitionID] + ownership
	}

	require.Equal(t, len(verification), len(ownershipByPartitionID))
	for partitionID, ownership := range ownershipByPartitionID {
		verifiedOwnership, ok := verification[partitionID]
		require.True(t, ok)
		require.Equal(t, ownership, verifiedOwnership)
	}

	return timeseriesOwnershipByToken, ownershipByPartitionID
}

func calculateOwnershipByPartitionID(ring *PartitionRing, timeseriesOwnershipByToken map[Token]float64) map[int32]float64 {
	ownershipByPartitionID := make(map[int32]float64, len(ring.Partitions()))

	for token, ownership := range timeseriesOwnershipByToken {
		partitionID := ring.partitionByToken[token]
		ownershipByPartitionID[partitionID] = ownershipByPartitionID[partitionID] + ownership
	}
	return ownershipByPartitionID
}

func printSimulation(t *testing.T, message string, ownershipByPartitionID map[int32]float64, verbose bool) {
	res := make(byCount, 0, len(ownershipByPartitionID))

	min := math.MaxFloat64
	max := 0.0
	for partitionID, ownership := range ownershipByPartitionID {
		min = math.Min(min, ownership)
		max = math.Max(max, ownership)

		res = append(res, instanceWithCount{
			instanceID:      fmt.Sprintf("%d", partitionID),
			timeseriesCount: int(ownership),
		})
	}

	if message != "" {
		fmt.Println(message)
	}
	fmt.Printf("min ownership: %10.3f, max ownership: %10.3f, spread: %10.3f%%\n", min, max, (1-min/max)*100)

	if verbose {
		slices.SortFunc(res, func(a, b instanceWithCount) bool {
			prefixA, err := strconv.Atoi(a.instanceID)
			require.NoError(t, err)
			prefixB, err := strconv.Atoi(b.instanceID)
			require.NoError(t, err)
			if prefixA < prefixB {
				return true
			}
			return false
		})
		for _, ic := range res {
			fmt.Printf("\tpartition %10s, %10d\n", ic.instanceID, ic.timeseriesCount)
		}
	}
}

func simulateTimeseriesDistribution(t *testing.T, dir string, cell string, zones []string, ring *PartitionRing, timeseriesAlreadyReplicated bool) map[Token]float64 {
	timeseriesOwnershipByToken := make(map[Token]float64, optimalTokensPerInstance*ring.activePartitionsCount)

	shardSizeByTenantID, err := GetShardSizeByTenantID(dir, cell)
	require.NoError(t, err)
	timeSeriesCountByTenantID, err := GetTimeseriesCountByTenantID(dir, cell)
	require.NoError(t, err)
	tenantCount := len(timeSeriesCountByTenantID)
	currCount := 0
	percentage := 10
	for tenantID, timeSeriesCount := range timeSeriesCountByTenantID {
		currCount++
		if currCount*100/tenantCount >= percentage {
			status := fmt.Sprintf("%s - %d%%", cell, percentage)
			fmt.Println(status)
			percentage += 10
		}
		shardSize, ok := shardSizeByTenantID[tenantID]
		if !ok {
			shardSize = 0
		}

		shardSize = shard2.ShuffleShardExpectedInstancesPerZone(shardSize, len(zones))

		shard, err := ring.ShuffleShard(tenantID, shardSize)
		require.NoError(t, err)

		timeSeriesCountBeforeReplication := timeSeriesCount
		if timeseriesAlreadyReplicated {
			timeSeriesCountBeforeReplication /= len(zones)
		}

		for j := 0; j < timeSeriesCountBeforeReplication; j++ {
			key := rand.Uint32()
			_, closestToken, err := shard.ActivePartitionForKey(key)
			require.NoError(t, err)
			timeseriesOwnershipByToken[closestToken] = timeseriesOwnershipByToken[closestToken] + 1
		}
	}
	fmt.Printf("\n%s completed\n", cell)
	return timeseriesOwnershipByToken
}

func compareAllShards(t *testing.T, first *PartitionRing, second *PartitionRing, tenantIDs []string, shardSizes []int) {
	for i := 0; i < len(tenantIDs); i++ {
		tenantID := tenantIDs[i]
		shardSize := shardSizes[i]
		firstShard, err := first.ShuffleShard(tenantID, shardSize)
		require.NoError(t, err)
		secondShard, err := second.ShuffleShard(tenantID, shardSize)
		require.NoError(t, err)
		added, removed := compareShards(firstShard, secondShard)
		require.LessOrEqual(t, len(added), 1)
		require.LessOrEqual(t, len(removed), 1)
	}
}

// compareShards returns the list of partition IDs which differ between the two instances of PartitionRing.
func compareShards(first, second *PartitionRing) (added, removed []int32) {
	for partitionID := range first.desc.Partitions {
		if _, ok := second.desc.Partitions[partitionID]; !ok {
			added = append(added, partitionID)
		}
	}

	for partitionID := range second.desc.Partitions {
		if _, ok := first.desc.Partitions[partitionID]; !ok {
			removed = append(removed, partitionID)
		}
	}

	return
}
