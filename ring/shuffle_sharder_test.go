package ring

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestNewHRWShuffleSharder(t *testing.T) {
	ring := createRealRing(t, "cp10-20240616")
	zones := ring.ringZones
	sharder := newHRWShuffleSharder(ring.ringDesc.GetIngesters(), zones, false)
	slices.Sort(zones)
	for _, zone := range zones {
		fmt.Printf("%s\n%s\n", strings.ToUpper(zone), strings.Repeat("-", 50))
		instances := sharder.instancesByZone[zone]
		slices.SortFunc(instances, func(a, b instanceHasher) bool {
			prefixA, idA, err := parseInstanceID(a.instanceID)
			require.NoError(t, err)
			prefixB, idB, err := parseInstanceID(b.instanceID)
			require.NoError(t, err)
			if prefixA < prefixB {
				return true
			}
			if prefixA == prefixB {
				return idA < idB
			}
			return false
		})
		for _, instance := range instances {
			fmt.Printf("instanceID: %30s, hash64: 0x%x\n", instance.instanceID, instance.hash64())
		}
	}
}

func TestIdentifierWithZone(t *testing.T) {
	identifier := "1234567"
	fmt.Printf("identifier: %10s, zone: %6s, hash64: 0x%x\n", identifier, "", hash64(identifier))
	fmt.Printf("identifier: %10s, zone: %6s, hash64: 0x%x\n", identifier, "", hash64(identifier, ""))
	for _, zone := range zones {
		fmt.Printf("identifier: %10s, zone: %6s, hash64: 0x%x\n", identifier, zone, hash64(identifier, zone))
	}
}

func TestHashSorter_Score(t *testing.T) {
	instancesPerZone := 5
	zones := zones
	instancesByZone := make(map[string][]instanceHasher, len(zones))
	for _, zone := range zones {
		instances := make([]instanceHasher, instancesPerZone)
		for i := 0; i < instancesPerZone; i++ {
			instanceID := fmt.Sprintf("instance-%s-%d", zone, i)
			instances[i] = instanceHasher{
				instanceID: instanceID,
				hash:       hash64(instanceID),
			}
		}
		instancesByZone[zone] = instances
	}
	sharder := hrwShuffleSharder{
		instancesByZone: instancesByZone,
		sorter:          hashSorter[instanceHasher]{},
	}
	identifier := "1234567"
	slices.Sort(zones)
	for _, zone := range zones {
		fmt.Printf("%s\n%s\n", strings.ToUpper(zone), strings.Repeat("-", 50))
		instances := sharder.instancesByZone[zone]
		for _, instance := range instances {
			fmt.Printf("instanceID: %30s (0x%x), identifier: %10s (0x%x), score: 0x%x\n", instance.instanceID, instance.hash64(), identifier, hash64(identifier, zone), instance.score(hash64(identifier, zone)))
		}

		fmt.Println("INSTANCES SORTED BY SCORE")

		sortedIdxs := sharder.sorter.sort(instances, hash64(identifier, zone))
		for _, idx := range sortedIdxs {
			fmt.Printf("instanceID: %30s, score: 0x%x\n", instances[idx].instanceID, instances[idx].score(hash64(identifier, zone)))
		}

	}
}
