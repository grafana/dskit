package ring

import (
	"fmt"
	"math/rand"
	"sort"

	shardUtil "github.com/grafana/dskit/ring/shard"
	"github.com/spaolacci/murmur3"
	"golang.org/x/exp/slices"
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

type weightedInstanceID struct {
	instanceID string
	weight     uint64
}

type byWeight []weightedInstanceID

func (w byWeight) Len() int      { return len(w) }
func (w byWeight) Swap(i, j int) { w[i], w[j] = w[j], w[i] }
func (w byWeight) Less(i, j int) bool {
	if w[i].weight > w[j].weight {
		return true
	}
	if w[i].weight == w[j].weight {
		return w[i].instanceID < w[j].instanceID
	}
	return false
}

type shuffleSharder interface {
	shuffleShard(
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
	return s.shuffleShardWithSMTRandomness(identifier, actualZones, numInstancesPerZone, ringTokensByZone, ringInstanceByToken, ringInstancesById, isWithinLookbackPeriod)
}

type monteCarloShuffleSharder struct {
	smtModeEnabled bool
}

func (s monteCarloShuffleSharder) shuffleShard(
	identifier string,
	actualZones []string,
	numInstancesPerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {
	if s.smtModeEnabled {
		return s.shuffleShardSMT(identifier, actualZones, numInstancesPerZone, ringTokensByZone, ringInstanceByToken, ringInstancesById, isWithinLookbackPeriod)
	}
	return s.shuffleShardRandom(identifier, actualZones, numInstancesPerZone, ringTokensByZone, ringInstanceByToken, ringInstancesById, isWithinLookbackPeriod)
}

func (s monteCarloShuffleSharder) shuffleShardRandom(
	identifier string,
	actualZones []string,
	numInstancesPerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {

	const attemptsPerInstance = 10000

	size := numInstancesPerZone * len(actualZones)
	shard := make(map[string]InstanceDesc, size)

	helpShardByZone := make(map[string]map[string]int, len(actualZones))
	for _, zone := range actualZones {
		helpShardByZone[zone] = make(map[string]int, numInstancesPerZone)
	}

	for _, zone := range actualZones {
		// Initialise the random generator used to select instances in the ring.
		// Since we use spread-minimizing firstZoneTokens, instances with the same id from
		// consecutive zones will have consecutive firstZoneTokens, we don't choose different
		// random generators for different zones. These random generators guarantee
		// the "consistency" property when the shard size changes or a new zone is added.
		random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(identifier, zone)))
		zoneTokens := ringTokensByZone[zone]
		for t := 0; t < attemptsPerInstance; t++ {
			tokenIdx := searchToken(zoneTokens, random.Uint32())
			tokenIdx %= len(zoneTokens)
			token := zoneTokens[tokenIdx]
			info, ok := ringInstanceByToken[token]
			if !ok {
				// This should never happen unless a bug in the ring code.
				panic(ErrInconsistentTokensInfo)
			}

			helpShard := helpShardByZone[zone]
			count, ok := helpShard[info.InstanceID]
			if !ok {
				count = 0
			}
			helpShard[info.InstanceID] = count + 1
			helpShardByZone[zone] = helpShard
		}
	}

	for _, helpShard := range helpShardByZone {
		c := make(byCount, 0, numInstancesPerZone)
		for instanceID, count := range helpShard {
			c = append(c, instanceWithCount{
				instanceID:      instanceID,
				timeseriesCount: count,
			})
		}
		sort.Sort(c)
		index := 0
		for i := 0; i < numInstancesPerZone; i++ {
			found := false
			for index < len(c) {
				instanceID := c[index].instanceID
				instance := ringInstancesById[instanceID]
				shard[instanceID] = instance
				index++

				// If the lookback is enabled and this instance has been registered within the lookback period
				// then we should include it in the subring but continuing selecting instances.
				if isWithinLookbackPeriod(instance.RegisteredTimestamp) {
					continue
				}
				found = true
				break
			}
			if !found {
				break
			}
		}
	}
	return &Desc{Ingesters: shard}
}

func (s monteCarloShuffleSharder) shuffleShardSMT(
	identifier string,
	actualZones []string,
	numInstancesPerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {

	const (
		attemptsPerInstance = 10000
	)

	size := numInstancesPerZone * len(actualZones)
	shard := make(map[string]InstanceDesc, size)

	slices.Sort(actualZones)
	// Initialise the random generator used to select instances in the ring.
	// Since we use spread-minimizing firstZoneTokens, instances with the same id from
	// consecutive zones will have consecutive firstZoneTokens, we don't choose different
	// random generators for different zones. These random generators guarantee
	// the "consistency" property when the shard size changes or a new zone is added.
	random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(identifier, actualZones[0])))

	firstZoneTokens := ringTokensByZone[actualZones[0]]
	helpShardByZone := make(map[string]map[string]int, len(actualZones))
	for _, zone := range actualZones {
		helpShardByZone[zone] = make(map[string]int, numInstancesPerZone)
	}
	for t := 0; t < attemptsPerInstance; t++ {
		tokenIdx := searchToken(firstZoneTokens, random.Uint32())
		tokenIdx %= len(firstZoneTokens)
		firstZoneToken := firstZoneTokens[tokenIdx]
		for z, zone := range actualZones {
			token := firstZoneToken + uint32(z)
			info, ok := ringInstanceByToken[token]
			if !ok {
				// This should never happen unless a bug in the ring code.
				panic(ErrInconsistentTokensInfo)
			}

			helpShard := helpShardByZone[zone]
			count, ok := helpShard[info.InstanceID]
			if !ok {
				count = 0
			}
			helpShard[info.InstanceID] = count + 1
			helpShardByZone[zone] = helpShard
		}
	}

	for _, helpShard := range helpShardByZone {
		c := make(byCount, 0, numInstancesPerZone)
		for instanceID, count := range helpShard {
			c = append(c, instanceWithCount{
				instanceID:      instanceID,
				timeseriesCount: count,
			})
		}
		sort.Sort(c)
		index := 0
		for i := 0; i < numInstancesPerZone; i++ {
			found := false
			for index < len(c) {
				instanceID := c[index].instanceID
				instance := ringInstancesById[instanceID]
				shard[instanceID] = instance
				index++

				// If the lookback is enabled and this instance has been registered within the lookback period
				// then we should include it in the subring but continuing selecting instances.
				if isWithinLookbackPeriod(instance.RegisteredTimestamp) {
					continue
				}
				found = true
				break
			}
			if !found {
				break
			}
		}
	}
	return &Desc{Ingesters: shard}
}

func (s randomShuffleSharder) shuffleShardWithSMTRandomness(
	identifier string,
	actualZones []string,
	numInstancesPerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {

	const (
		attemptsPerInstance = 10000
	)

	size := numInstancesPerZone * len(actualZones)
	shard := make(map[string]InstanceDesc, size)

	// Initialise the random generator used to select instances in the ring.
	// Since we use spread-minimizing firstZoneTokens, instances with the same id from
	// consecutive zones will have consecutive firstZoneTokens, we don't choose different
	// random generators for different zones. These random generators guarantee
	// the "consistency" property when the shard size changes or a new zone is added.
	random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(identifier, "")))

	slices.Sort(actualZones)
	firstZoneTokens := ringTokensByZone[actualZones[0]]
	helpShardByZone := make(map[string]map[string]int, len(actualZones))
	for _, zone := range actualZones {
		helpShardByZone[zone] = make(map[string]int, numInstancesPerZone)
	}
	for t := 0; t < attemptsPerInstance; t++ {
		tokenIdx := searchToken(firstZoneTokens, random.Uint32())
		tokenIdx %= len(firstZoneTokens)
		firstZoneToken := firstZoneTokens[tokenIdx]
		for z, zone := range actualZones {
			token := firstZoneToken + uint32(z)
			info, ok := ringInstanceByToken[token]
			if !ok {
				// This should never happen unless a bug in the ring code.
				panic(ErrInconsistentTokensInfo)
			}

			helpShard := helpShardByZone[zone]
			count, ok := helpShard[info.InstanceID]
			if !ok {
				count = 0
			}
			helpShard[info.InstanceID] = count + 1
			helpShardByZone[zone] = helpShard
		}
	}

	for _, helpShard := range helpShardByZone {
		c := make(byCount, 0, numInstancesPerZone)
		for instanceID, count := range helpShard {
			c = append(c, instanceWithCount{
				instanceID:      instanceID,
				timeseriesCount: count,
			})
		}
		sort.Sort(c)
		index := 0
		for i := 0; i < numInstancesPerZone; i++ {
			found := false
			for index < len(c) {
				instanceID := c[index].instanceID
				instance := ringInstancesById[instanceID]
				shard[instanceID] = instance
				index++

				// If the lookback is enabled and this instance has been registered within the lookback period
				// then we should include it in the subring but continuing selecting instances.
				if isWithinLookbackPeriod(instance.RegisteredTimestamp) {
					continue
				}
				found = true
				break
			}
			if !found {
				break
			}
		}
	}
	return &Desc{Ingesters: shard}
}

func (s randomShuffleSharder) shuffleShardWithRandomness(
	identifier string,
	actualZones []string,
	numInstancesPerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {

	const (
		attemptsPerInstance = 10000
	)

	size := numInstancesPerZone * len(actualZones)
	shard := make(map[string]InstanceDesc, size)

	helpShardByZone := make(map[string]map[string]int, len(actualZones))
	for _, zone := range actualZones {
		helpShardByZone[zone] = make(map[string]int, numInstancesPerZone)
	}

	for _, zone := range actualZones {
		// Initialise the random generator used to select instances in the ring.
		// Since we use spread-minimizing firstZoneTokens, instances with the same id from
		// consecutive zones will have consecutive firstZoneTokens, we don't choose different
		// random generators for different zones. These random generators guarantee
		// the "consistency" property when the shard size changes or a new zone is added.
		random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(identifier, zone)))
		zoneTokens := ringTokensByZone[zone]
		for t := 0; t < attemptsPerInstance; t++ {
			tokenIdx := searchToken(zoneTokens, random.Uint32())
			tokenIdx %= len(zoneTokens)
			token := zoneTokens[tokenIdx]
			info, ok := ringInstanceByToken[token]
			if !ok {
				// This should never happen unless a bug in the ring code.
				panic(ErrInconsistentTokensInfo)
			}

			helpShard := helpShardByZone[zone]
			count, ok := helpShard[info.InstanceID]
			if !ok {
				count = 0
			}
			helpShard[info.InstanceID] = count + 1
			helpShardByZone[zone] = helpShard
		}
	}

	for _, helpShard := range helpShardByZone {
		c := make(byCount, 0, numInstancesPerZone)
		for instanceID, count := range helpShard {
			c = append(c, instanceWithCount{
				instanceID:      instanceID,
				timeseriesCount: count,
			})
		}
		sort.Sort(c)
		index := 0
		for i := 0; i < numInstancesPerZone; i++ {
			found := false
			for index < len(c) {
				instanceID := c[index].instanceID
				instance := ringInstancesById[instanceID]
				shard[instanceID] = instance
				index++

				// If the lookback is enabled and this instance has been registered within the lookback period
				// then we should include it in the subring but continuing selecting instances.
				if isWithinLookbackPeriod(instance.RegisteredTimestamp) {
					continue
				}
				found = true
				break
			}
			if !found {
				break
			}
		}
	}
	return &Desc{Ingesters: shard}
}

type hasher interface {
	id() string
	hash64() uint64
	score(uint64) uint64
}

type instanceHasher struct {
	instanceID string
	hash       uint64
}

func (i instanceHasher) id() string {
	return i.instanceID
}

func (i instanceHasher) hash64() uint64 {
	return i.hash
}

func (i instanceHasher) score(key uint64) uint64 {
	s := i.hash64() ^ key
	// here used mmh3 64 bit finalizer
	// https://github.com/aappleby/smhasher/blob/61a0530f28277f2e850bfc39600ce61d02b518de/src/MurmurHash3.cpp#L81
	s ^= s >> 33
	s = s * 0xff51afd7ed558ccd
	s ^= s >> 33
	s = s * 0xc4ceb9fe1a85ec53
	s ^= s >> 33
	return s
}

type hashSorter[T hasher] struct{}

// sort gets a slice of elements of type hasher and a key,
// for each element of the slice computes a score as a
// combination of element's hash value and the given key,
// sorts the scores of the input elements in descending
// order, and returns and the slice of indexes of the input
// slice in such a way that the first element of the result
// is the index of the element from the input slice with the
// highest score.
func (hs hashSorter[T]) sort(nodesWithHash []T, key uint64) []int {
	indexes := make([]int, len(nodesWithHash))
	for i := 0; i < len(indexes); i++ {
		indexes[i] = i
	}

	slices.SortFunc(indexes, func(a, b int) bool {
		scoreA := nodesWithHash[a].score(key)
		scoreB := nodesWithHash[b].score(key)
		if scoreA > scoreB {
			return true
		}
		if scoreA == scoreB {
			return nodesWithHash[a].id() <= nodesWithHash[b].id()
		}
		return false
	})
	return indexes
}

type hrwShuffleSharder struct {
	instancesByZone map[string][]instanceHasher
	sorter          hashSorter[instanceHasher]
	smtModeEnabled  bool
}

func hash64(args ...string) uint64 {
	if len(args) == 0 {
		return 0
	}
	if len(args) == 1 {
		return murmur3.Sum64([]byte(args[0]))
	}
	hash := murmur3.New64()
	for _, arg := range args {
		hash.Write([]byte(arg))
	}
	return hash.Sum64()
}

func newHRWShuffleSharder(instances map[string]InstanceDesc, zones []string, smtModeEnabled bool) hrwShuffleSharder {
	instancesByZone := make(map[string][]instanceHasher, len(zones))
	instancesPerZone := len(instances) / len(zones)
	for instanceID, id := range instances {
		instances, ok := instancesByZone[id.Zone]
		if !ok {
			instances = make([]instanceHasher, 0, instancesPerZone)
		}
		instances = append(instances, instanceHasher{
			instanceID: instanceID,
			hash:       hash64(instanceID),
		})
		instancesByZone[id.Zone] = instances
	}
	return hrwShuffleSharder{
		instancesByZone: instancesByZone,
		sorter:          hashSorter[instanceHasher]{},
		smtModeEnabled:  smtModeEnabled,
	}
}

func (s hrwShuffleSharder) shuffleShard(
	identifier string,
	actualZones []string,
	numInstancesPerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {
	if s.smtModeEnabled {
		return s.shuffleShardSMT(identifier, actualZones, numInstancesPerZone, ringTokensByZone, ringInstanceByToken, ringInstancesById, isWithinLookbackPeriod)
	}
	return s.shuffleShardRandom(identifier, actualZones, numInstancesPerZone, ringTokensByZone, ringInstanceByToken, ringInstancesById, isWithinLookbackPeriod)
}

func (s hrwShuffleSharder) shuffleShardRandom(
	identifier string,
	actualZones []string,
	numInstancesPerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {

	size := numInstancesPerZone * len(actualZones)
	shard := make(map[string]InstanceDesc, size)

	for zone, instances := range s.instancesByZone {
		identifierHash64 := hash64(identifier, zone)
		sortedIdxs := s.sorter.sort(instances, identifierHash64)

		addedInstances := 0
		currentIdx := 0
		for addedInstances < numInstancesPerZone && currentIdx < len(sortedIdxs) {
			hI := instances[sortedIdxs[currentIdx]]
			currentIdx++
			instance := ringInstancesById[hI.instanceID]
			shard[hI.instanceID] = instance

			// If the lookback is enabled and this instance has been registered within the lookback period
			// then we should include it in the subring but continuing selecting instances.
			if isWithinLookbackPeriod(instance.RegisteredTimestamp) {
				continue
			}
			addedInstances++
		}
	}

	return &Desc{Ingesters: shard}
}

func (s hrwShuffleSharder) shuffleShardSMT(
	identifier string,
	actualZones []string,
	numInstancesPerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {

	size := numInstancesPerZone * len(actualZones)
	shard := make(map[string]InstanceDesc, size)

	identifierHash64 := hash64(identifier, actualZones[0])
	slices.Sort(actualZones)
	firstZoneInstances := s.instancesByZone[actualZones[0]]
	firstZoneSortedIdxs := s.sorter.sort(firstZoneInstances, identifierHash64)
	for z := range actualZones {
		addedInstances := 0
		currentIdx := 0
		for addedInstances < numInstancesPerZone && currentIdx < len(firstZoneSortedIdxs) {
			firstZoneInstance := firstZoneInstances[firstZoneSortedIdxs[currentIdx]]
			firstZoneInstanceToken := ringInstancesById[firstZoneInstance.instanceID].Tokens[0]
			currentZoneToken := firstZoneInstanceToken + uint32(z)
			currentZoneInstance := ringInstanceByToken[currentZoneToken]
			currentIdx++
			instance := ringInstancesById[currentZoneInstance.InstanceID]
			shard[currentZoneInstance.InstanceID] = instance

			// If the lookback is enabled and this instance has been registered within the lookback period
			// then we should include it in the subring but continuing selecting instances.
			if isWithinLookbackPeriod(instance.RegisteredTimestamp) {
				continue
			}
			addedInstances++
		}
	}

	return &Desc{Ingesters: shard}
}
