package ring

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"golang.org/x/exp/slices"
)

func TestExperimentalSpreadMinimizingShuffleShard(t *testing.T) {
	instancesPerZone := 5
	tenants := []string{"tenant-1", "tenant-2", "tenant-3", "tenant-4", "tenant-5"}
	sharder := newSpreadMinimizingShuffleSharder()
	for _, tenant := range tenants {
		for shardSizePerZone := 1; shardSizePerZone <= instancesPerZone; shardSizePerZone++ {
			fmt.Printf("SHARD SIZE: %d OF %d FOR TENANT %s\n", shardSizePerZone*len(zones), instancesPerZone*len(zones), tenant)
			fmt.Println(strings.Repeat("-", 30))
			instanceByToken, tokensByZone, instanceById := createSharderInputParameters(t, tokensPerInstance, instancesPerZone)
			tenantID := "tenant-1"
			isWithinLookbackPeriod := func(timestamp int64) bool { return false }
			shardDesc := sharder.shuffleShard(tenantID, zones, shardSizePerZone, tokensByZone, instanceByToken, instanceById, isWithinLookbackPeriod)
			instances := make([]string, 0, len(shardDesc.GetTokens()))
			for _, instance := range shardDesc.GetIngesters() {
				instances = append(instances, instance.Id)
			}
			slices.Sort(instances)
			fmt.Println(instances)

			ownershipByInstanceByZone := tokenOwnershipByZone(shardDesc, shardSizePerZone)
			for zone, ownershipByInstance := range ownershipByInstanceByZone {
				var (
					min = math.MaxFloat64
					max = 0.0
				)
				for _, ownership := range ownershipByInstance {
					min = math.Min(min, ownership)
					max = math.Max(max, ownership)
				}
				fmt.Printf("Spread of zone \"%s\" is %.3f\n", zone, 1-min/max)
			}
		}
	}
}

func TestExperimentalSpreadMinimizingShuffleSharderWithOptimizations(t *testing.T) {
	instancesPerZone := 50
	tenants := []string{"tenant-1", "tenant-2", "tenant-3", "tenant-4", "tenant-5"}
	sharder := newSpreadMinimizingShuffleSharder()
	for _, tenant := range tenants {
		for shardSizePerZone := 1; shardSizePerZone <= instancesPerZone; shardSizePerZone++ {
			fmt.Printf("SHARD SIZE: %d OF %d FOR TENANT %s\n", shardSizePerZone*len(zones), instancesPerZone*len(zones), tenant)
			fmt.Println(strings.Repeat("-", 30))
			instanceByToken, tokensByZone, instanceById := createSharderInputParameters(t, tokensPerInstance, instancesPerZone)
			isWithinLookbackPeriod := func(timestamp int64) bool { return false }
			shardDesc := sharder.shuffleShardNew(tenant, zones, shardSizePerZone, tokensByZone, instanceByToken, instanceById, isWithinLookbackPeriod)
			instances := make([]string, 0, len(shardDesc.GetTokens()))
			for _, instance := range shardDesc.GetIngesters() {
				instances = append(instances, instance.Id)
			}
			slices.Sort(instances)
			fmt.Println(instances)

			ownershipByInstanceByZone := tokenOwnershipByZone(shardDesc, shardSizePerZone)
			for zone, ownershipByInstance := range ownershipByInstanceByZone {
				var (
					min = math.MaxFloat64
					max = 0.0
				)
				for _, ownership := range ownershipByInstance {
					min = math.Min(min, ownership)
					max = math.Max(max, ownership)
				}
				fmt.Printf("Spread of zone \"%s\" is %.3f\n", zone, 1-min/max)
			}
		}
	}
}
