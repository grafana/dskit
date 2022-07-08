package ring

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestInvestigateUnbalanceSeriesPerIngester(t *testing.T) {
	now := time.Now().Unix()
	desc := unbalancedSeriesRingDesc

	// Update the ring to ensure all instances are ACTIVE and healthy.
	for id, instance := range desc.Ingesters {
		instance.State = ACTIVE
		instance.Timestamp = now
		desc.Ingesters[id] = instance
	}

	// Create a ring with the instances.
	ring := Ring{
		cfg: Config{
			HeartbeatTimeout:     time.Hour,
			ReplicationFactor:    3,
			ZoneAwarenessEnabled: true,
		},
		ringDesc:            desc,
		ringTokens:          desc.GetTokens(),
		ringTokensByZone:    desc.getTokensByZone(),
		ringInstanceByToken: desc.getTokensInfo(),
		ringZones:           getZones(desc.getTokensByZone()),
		strategy:            NewDefaultReplicationStrategy(),
	}

	// Compute statistics about % of owned tokens GLOBALLY.
	minOwnedPercentage, maxOwnedPercentage, maxVariance := computeMinAndMaxTokensOwnership(desc)
	fmt.Println(fmt.Sprintf("Global ownership: min=%.2f%% max=%.2f%% max variance=%.2f%%", minOwnedPercentage, maxOwnedPercentage, maxVariance))

	// Compute statistics about % of owned tokens PER-ZONE.
	// To compute it we need to only take in account ingesters in that zone.
	for _, zone := range ring.ringZones {
		// Build a ring description including only instances from the given zone.
		zoneDesc := &Desc{Ingesters: map[string]InstanceDesc{}}
		for id, instance := range desc.Ingesters {
			if instance.Zone == zone {
				zoneDesc.Ingesters[id] = instance
			}
		}

		minOwnedPercentage, maxOwnedPercentage, maxVariance := computeMinAndMaxTokensOwnership(zoneDesc)
		fmt.Println(fmt.Sprintf("Per-zone ownership: zone=%s min=%.2f%% max=%.2f%% max variance=%.2f%%", zone, minOwnedPercentage, maxOwnedPercentage, maxVariance))
	}
}

func computeMinAndMaxTokensOwnership(desc *Desc) (float64, float64, float64) {
	minOwnedPercentage := math.MaxFloat64
	maxOwnedPercentage := float64(0)

	for _, numTokens := range desc.countTokens() {
		ownedPercentage := (float64(numTokens) / float64(math.MaxUint32)) * 100
		if ownedPercentage < minOwnedPercentage {
			minOwnedPercentage = ownedPercentage
		}
		if ownedPercentage > maxOwnedPercentage {
			maxOwnedPercentage = ownedPercentage
		}
	}

	maxVariance := ((maxOwnedPercentage - minOwnedPercentage) / maxOwnedPercentage) * 100

	return minOwnedPercentage, maxOwnedPercentage, maxVariance
}
