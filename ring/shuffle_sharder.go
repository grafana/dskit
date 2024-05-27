package ring

import (
	"fmt"
	"math/rand"

	shardUtil "github.com/grafana/dskit/ring/shard"
)

const (
	defaultZone = ""
)

var (
	errSingleZoneShardCapacityExceeded = func(capacity int) error {
		return fmt.Errorf("it was impossible to add an instance to a singleZoneShard, because its capacity %d would be exceeded", capacity)
	}
	errSingleZoneShardInstanceAlreadyPresent = func(instanceID string) error {
		return fmt.Errorf("it was impossible to add an instance to a singleZoneShard: instance %s already present", instanceID)
	}
	errSingleZoneShardInstanceNotPresent = func(instanceID string) error {
		return fmt.Errorf("it was impossible to remove an instance from a singleZoneShard: instance %s is not present present", instanceID)
	}
	errImpossibleToFineTheBestSingleZoneShard = func(shardSize int) error {
		return fmt.Errorf("it was impossible to find a singleZoneShard of size %d", shardSize)
	}
)

type shuffleSharder interface {
	shuffleShard(
		identifier string,
		actualZones []string,
		numInstancesPerZone int,
		ringTokensByZone map[string][]uint32,
		ringInstanceByToken map[uint32]instanceInfo,
		ringInstancesById map[string]InstanceDesc,
		isWithinLookbackPeriod func(int64) bool) *Desc
	shuffleShardNew(
		identifier string,
		actualZones []string,
		numInstancesPerZone int,
		ringTokensByZone map[string][]uint32,
		ringInstanceByToken map[uint32]instanceInfo,
		ringInstancesById map[string]InstanceDesc,
		isWithinLookbackPeriod func(int64) bool) *Desc
}

type randomShuffleSharder struct{}

func (s randomShuffleSharder) shuffleShard(
	identifier string,
	actualZones []string,
	numInstancesPerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {

	size := numInstancesPerZone * len(actualZones)
	shard := make(map[string]InstanceDesc, size)

	// We need to iterate zones always in the same order to guarantee stability.
	for _, zone := range actualZones {
		tokens := ringTokensByZone[zone]

		// Initialise the random generator used to select instances in the ring.
		// Since we consider each zone like an independent ring, we have to use dedicated
		// pseudo-random generator for each zone, in order to guarantee the "consistency"
		// property when the shard size changes or a new zone is added.
		random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(identifier, zone)))

		// To select one more instance while guaranteeing the "consistency" property,
		// we do pick a random value from the generator and resolve uniqueness collisions
		// (if any) continuing walking the ring.
		for i := 0; i < numInstancesPerZone; i++ {
			start := searchToken(tokens, random.Uint32())
			iterations := 0
			found := false

			for p := start; iterations < len(tokens); p++ {
				iterations++

				// Wrap p around in the ring.
				p %= len(tokens)

				info, ok := ringInstanceByToken[tokens[p]]
				if !ok {
					// This should never happen unless a bug in the ring code.
					panic(ErrInconsistentTokensInfo)
				}

				// Ensure we select a unique instance.
				if _, ok := shard[info.InstanceID]; ok {
					continue
				}

				instanceID := info.InstanceID
				instance := ringInstancesById[instanceID]
				shard[instanceID] = instance

				// If the lookback is enabled and this instance has been registered within the lookback period
				// then we should include it in the subring but continuing selecting instances.
				if isWithinLookbackPeriod(instance.RegisteredTimestamp) {
					continue
				}

				found = true
				break
			}

			// If one more instance has not been found, we can stop looking for
			// more instances in this zone, because it means the zone has no more
			// instances which haven't been already selected.
			if !found {
				break
			}
		}
	}

	return &Desc{Ingesters: shard}
}

func (s randomShuffleSharder) shuffleShardNew(
	identifier string,
	actualZones []string,
	numInstancesPerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {
	return s.shuffleShard(identifier, actualZones, numInstancesPerZone, ringTokensByZone, ringInstanceByToken, ringInstancesById, isWithinLookbackPeriod)
}
