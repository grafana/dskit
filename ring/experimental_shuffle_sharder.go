package ring

import (
	"container/list"
	"fmt"
	"math"
	"math/rand"

	shardUtil "github.com/grafana/dskit/ring/shard"
	"golang.org/x/exp/slices"
)

type spreadMinimizingShuffleSharder struct {
	cachedFirstZoneShards map[string]map[int]singleZoneShard
}

func newSpreadMinimizingShuffleSharder() *spreadMinimizingShuffleSharder {
	return &spreadMinimizingShuffleSharder{
		cachedFirstZoneShards: map[string]map[int]singleZoneShard{},
	}
}

func (s *spreadMinimizingShuffleSharder) shuffleShard(
	identifier string,
	actualZones []string,
	shardSizePerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {
	slices.Sort(actualZones)
	firstZone := actualZones[0]
	// Initialise the random generator used to select instances in the ring.
	// Since we consider each zone like an independent ring, we have to use dedicated
	// pseudo-random generator for each zone, in order to guarantee the "consistency"
	// property when the shard size changes or a new zone is added.
	random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(identifier, "")))
	tokens := ringTokensByZone[firstZone]
	start := searchToken(tokens, random.Uint32())
	// Wrap start around in the ring.
	start %= len(tokens)

	info, ok := ringInstanceByToken[tokens[start]]
	if !ok {
		// This should never happen unless a bug in the ring code.
		panic(ErrInconsistentTokensInfo)
	}

	instanceID := info.InstanceID

	var (
		firstZoneShard            singleZoneShard
		currentFirstZoneShardSize int
	)
	if cachedFirstZoneShardBySize, ok := s.cachedFirstZoneShards[instanceID]; ok {
		for currSize := shardSizePerZone; currSize > 1; currSize-- {
			if cachedFirstZoneShard, ok := cachedFirstZoneShardBySize[currSize]; ok {
				shard := make(map[string]InstanceDesc, len(cachedFirstZoneShard.shard))
				for instanceID, instance := range cachedFirstZoneShard.shard {
					shard[instanceID] = instance
				}
				tokens := make(Tokens, len(cachedFirstZoneShard.tokens))
				copy(tokens, cachedFirstZoneShard.tokens)
				firstZoneShard = singleZoneShard{
					zone:            firstZone,
					shard:           shard,
					tokens:          tokens,
					instanceByToken: ringInstanceByToken,
				}
				currentFirstZoneShardSize = currSize
				break
			}
		}
	}

	if currentFirstZoneShardSize == 0 {
		s.cachedFirstZoneShards[instanceID] = map[int]singleZoneShard{}
		firstZoneShard = newSingleZoneShard(firstZone, ringInstanceByToken)
		firstZoneShard.addInstance(ringInstancesById[instanceID])
		currentFirstZoneShardSize = 1
	}

	for i := currentFirstZoneShardSize; i < shardSizePerZone; i++ {
		var (
			bestCandidateFound = false
			bestCandidate      InstanceDesc
			minSpread          = math.MaxFloat64
		)
		for {
			for _, candidateInstance := range ringInstancesById {
				// We consider only the instances from the first zone.
				// Instances from all other zones follow the ones from
				// the first zone.
				if candidateInstance.Zone != firstZone {
					continue
				}
				if firstZoneShard.contains(candidateInstance) {
					continue
				}
				spread := firstZoneShard.calculateSpreadWithCandidate(&candidateInstance)
				if spread < minSpread {
					bestCandidate = candidateInstance
					bestCandidateFound = true
				}
			}
			if !bestCandidateFound {
				// This should never happen unless a bug in the ring code.
				panic(ErrInstanceNotFound)
			}
			firstZoneShard.addInstance(bestCandidate)

			// If the lookback is enabled and the best candidate has been registered within the lookback period
			// then we should include it in the subring but continuing selecting instances.
			if isWithinLookbackPeriod(bestCandidate.RegisteredTimestamp) {
				continue
			}
			break
		}
	}

	s.cachedFirstZoneShards[instanceID][shardSizePerZone] = firstZoneShard

	// We now add instances from other zones that correspond to the instances from the first zone shard.
	return s.createRingDescFromTheFirstZoneShard(firstZoneShard, actualZones, ringInstanceByToken, ringInstancesById)
}

func (s *spreadMinimizingShuffleSharder) shuffleShardNew(
	identifier string,
	actualZones []string,
	shardSizePerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesById map[string]InstanceDesc,
	isWithinLookbackPeriod func(int64) bool) *Desc {
	slices.Sort(actualZones)
	firstZone := actualZones[0]
	// Initialise the random generator used to select instances in the ring.
	// Since we consider each zone like an independent ring, we have to use dedicated
	// pseudo-random generator for each zone, in order to guarantee the "consistency"
	// property when the shard size changes or a new zone is added.
	random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(identifier, "")))
	tokens := ringTokensByZone[firstZone]
	start := searchToken(tokens, random.Uint32())
	// Wrap start around in the ring.
	start %= len(tokens)

	info, ok := ringInstanceByToken[tokens[start]]
	if !ok {
		// This should never happen unless a bug in the ring code.
		panic(ErrInconsistentTokensInfo)
	}

	instanceID := info.InstanceID
	instancesPerZone := len(ringInstancesById) / len(actualZones)
	firstZoneShard, err := s.findOptimalSingleZoneShard(firstZone, instanceID, shardSizePerZone, instancesPerZone, ringInstanceByToken, ringInstancesById, isWithinLookbackPeriod)
	if err != nil {
		// This should never happen unless there's a bug in the ring code.
		panic(err)
	}

	fmt.Printf("\tspread for tenant %s and size %d is %.3f\n", identifier, shardSizePerZone*len(actualZones), firstZoneShard.getSpread())

	// We now add instances from other zones that correspond to the instances from the first zone shard.
	return s.createRingDescFromTheFirstZoneShard(firstZoneShard, actualZones, ringInstanceByToken, ringInstancesById)
}

func (s *spreadMinimizingShuffleSharder) optimalSingleZoneShard(zone string, firstInstanceID string, shardSize int, numberOfInstances int, ringInstanceByToken map[uint32]instanceInfo, ringInstanceByID map[string]InstanceDesc, isWithinLookbackPeriod func(int64) bool) (singleZoneShard, error) {
	if _, ok := s.cachedFirstZoneShards[firstInstanceID]; !ok {
		s.cachedFirstZoneShards[firstInstanceID] = make(map[int]singleZoneShard)
	}
	if result, ok := s.cachedFirstZoneShards[firstInstanceID][shardSize]; ok {
		return result, nil
	}

	currentSingleZoneShard := newSingleZoneShard(zone, ringInstanceByToken)
	firstInstanceDesc := ringInstanceByID[firstInstanceID]
	currentSingleZoneShard.addInstance(firstInstanceDesc)

	if shardSize == 1 {
		s.cachedFirstZoneShards[firstInstanceID][shardSize] = currentSingleZoneShard
		return currentSingleZoneShard, nil
	}

	orderedIntanceDescs := make([]InstanceDesc, 0, numberOfInstances-1)

	var (
		currentInstanceID = firstInstanceID
		err               error
	)
	for {
		currentInstanceID, err = nextInstanceID(currentInstanceID, numberOfInstances)
		if err != nil {
			return currentSingleZoneShard, err
		}
		if currentInstanceID == firstInstanceID {
			break
		}
		orderedIntanceDescs = append(orderedIntanceDescs, ringInstanceByID[currentInstanceID])
	}

	remainingShardSize := shardSize
	if !isWithinLookbackPeriod(ringInstanceByID[firstInstanceID].RegisteredTimestamp) {
		remainingShardSize--
	}
	bestSingleZoneShard := s.optimalSingleZoneShardRec(currentSingleZoneShard, nil, remainingShardSize, orderedIntanceDescs, 0, isWithinLookbackPeriod)

	if bestSingleZoneShard == nil {
		return currentSingleZoneShard, errImpossibleToFineTheBestSingleZoneShard(shardSize)
	}

	s.cachedFirstZoneShards[firstInstanceID][shardSize] = *bestSingleZoneShard
	return *bestSingleZoneShard, nil
}

// if we start from the instance with id 3, and there are in total 10 instances (with ids 0, 1, ..., 9), our ids slice should be [4, 5, 6, 7, 8, 9, 0, 1, 2]
func (s *spreadMinimizingShuffleSharder) optimalSingleZoneShardRec(currentShard singleZoneShard, currentBestShard *singleZoneShard, remainingShardSize int, orderedInstanceDescs []InstanceDesc, currentInstanceDescID int, isWithinLookbackPeriod func(int64) bool) *singleZoneShard {
	if remainingShardSize <= 0 {
		if currentBestShard == nil || currentShard.getSpread() < currentBestShard.getSpread() {
			currentBestShard = &currentShard
		}
		return currentBestShard
	}

	if currentInstanceDescID >= len(orderedInstanceDescs) {
		return currentBestShard
	}

	for idx := currentInstanceDescID; idx < len(orderedInstanceDescs); idx++ {
		currentInstanceDesc := orderedInstanceDescs[idx]
		// at this point currentInstanceID is surely not into currentShard
		nextShard := currentShard.deepCopy()
		nextShard.addInstance(currentInstanceDesc)

		remainingSize := remainingShardSize
		if !isWithinLookbackPeriod(currentInstanceDesc.RegisteredTimestamp) {
			remainingSize--
		}
		currentBestShard = s.optimalSingleZoneShardRec(nextShard, currentBestShard, remainingSize, orderedInstanceDescs, idx+1, isWithinLookbackPeriod)
	}
	return currentBestShard
}

func (s *spreadMinimizingShuffleSharder) addTokensToTheList(tokenList *list.List, tokens []uint32) {
	for _, token := range tokens {
		tokenList.PushBack(token)
	}
}

func (s *spreadMinimizingShuffleSharder) findOptimalSingleZoneShard(zone string, firstInstanceID string, shardSize int, numberOfInstances int, ringInstanceByToken map[uint32]instanceInfo, ringInstanceByID map[string]InstanceDesc, isWithinLookbackPeriod func(int64) bool) (singleZoneShard, error) {
	if _, ok := s.cachedFirstZoneShards[firstInstanceID]; !ok {
		s.cachedFirstZoneShards[firstInstanceID] = make(map[int]singleZoneShard)
	}
	if result, ok := s.cachedFirstZoneShards[firstInstanceID][shardSize]; ok {
		return result, nil
	}

	var err error
	tOwnership := newTokenOwnership(numberOfInstances)
	currentSingleZoneShard := newSingleZoneShard(zone, ringInstanceByToken)
	firstInstanceDesc := ringInstanceByID[firstInstanceID]
	_, tOwnership, err = currentSingleZoneShard.add(firstInstanceDesc, tOwnership)
	if err != nil {
		return currentSingleZoneShard, err
	}

	if shardSize == 1 {
		s.cachedFirstZoneShards[firstInstanceID][shardSize] = currentSingleZoneShard
		return currentSingleZoneShard, nil
	}

	orderedIntanceDescs := make([]InstanceDesc, 0, numberOfInstances-1)

	var (
		currentInstanceID = firstInstanceID
	)
	for {
		currentInstanceID, err = nextInstanceID(currentInstanceID, numberOfInstances)
		if err != nil {
			return currentSingleZoneShard, err
		}
		if currentInstanceID == firstInstanceID {
			break
		}
		orderedIntanceDescs = append(orderedIntanceDescs, ringInstanceByID[currentInstanceID])
	}

	remainingShardSize := shardSize
	if !isWithinLookbackPeriod(ringInstanceByID[firstInstanceID].RegisteredTimestamp) {
		remainingShardSize--
	}
	bestSingleZoneShard := s.findOptimalSingleZoneShardRec(currentSingleZoneShard, nil, remainingShardSize, tOwnership, orderedIntanceDescs, 0, isWithinLookbackPeriod)

	if bestSingleZoneShard == nil {
		return currentSingleZoneShard, errImpossibleToFineTheBestSingleZoneShard(shardSize)
	}

	s.cachedFirstZoneShards[firstInstanceID][shardSize] = *bestSingleZoneShard
	return *bestSingleZoneShard, nil
}

// if we start from the instance with id 3, and there are in total 10 instances (with ids 0, 1, ..., 9), our ids slice should be [4, 5, 6, 7, 8, 9, 0, 1, 2]
func (s *spreadMinimizingShuffleSharder) findOptimalSingleZoneShardRec(currentShard singleZoneShard, currentBestShard *singleZoneShard, remainingShardSize int, tOwnership *tokenOwnership, orderedInstanceDescs []InstanceDesc, currentInstanceDescID int, isWithinLookbackPeriod func(int64) bool) *singleZoneShard {
	if remainingShardSize <= 0 {
		if currentBestShard == nil || currentShard.getSpread() < currentBestShard.getSpread() {
			bestShard := currentShard.copy()
			currentBestShard = &bestShard
		}
		return currentBestShard
	}

	if currentInstanceDescID >= len(orderedInstanceDescs) {
		return currentBestShard
	}

	var (
		addedTokenListElementByToken []*list.Element
		err                          error
	)

	nextShard := currentShard.copy()
	for idx := currentInstanceDescID; idx <= len(orderedInstanceDescs)-remainingShardSize; idx++ {
		currentInstanceDesc := orderedInstanceDescs[idx]
		// at this point currentInstanceID is surely not into currentShard
		addedTokenListElementByToken, tOwnership, err = nextShard.add(currentInstanceDesc, tOwnership)
		if err != nil {
			return nil
		}

		remainingSize := remainingShardSize
		if !isWithinLookbackPeriod(currentInstanceDesc.RegisteredTimestamp) {
			remainingSize--
		}
		currentBestShard = s.findOptimalSingleZoneShardRec(nextShard, currentBestShard, remainingSize, tOwnership, orderedInstanceDescs, idx+1, isWithinLookbackPeriod)

		tOwnership, err = nextShard.remove(currentInstanceDesc, tOwnership, addedTokenListElementByToken)
		if err != nil {
			return nil
		}
	}
	return currentBestShard
}

func (s *spreadMinimizingShuffleSharder) createRingDescFromTheFirstZoneShard(firstZoneShard singleZoneShard, sortedZones []string, ringInstanceByToken map[uint32]instanceInfo, ringInstancesById map[string]InstanceDesc) *Desc {
	shard := make(map[string]InstanceDesc, len(sortedZones)*len(firstZoneShard.shard))
	for _, instanceFromFirstZone := range firstZoneShard.shard {
		shard[instanceFromFirstZone.Id] = instanceFromFirstZone

		// We now add instances from other zones that correspond to instanceFromFirstZone.
		// In spread-minimizing token strategy tokens of the former are direct successors
		// of instanceFromFirstZone's tokens.
		// It is enough to add the instances corresponding to the first (len(actualZones) - 1)
		// successors of the first token of instanceFromFirstZone.
		tokens := instanceFromFirstZone.GetTokens()
		for z := 1; z < len(sortedZones); z++ {
			token := tokens[0] + uint32(z)
			instanceFromNextZone := ringInstanceByToken[token]
			shard[instanceFromNextZone.InstanceID] = ringInstancesById[instanceFromNextZone.InstanceID]
		}
	}

	return &Desc{Ingesters: shard}
}

type singleZoneShard struct {
	zone            string
	shard           map[string]InstanceDesc
	tokens          Tokens
	instanceByToken map[uint32]instanceInfo
	tokenOwnership  map[string]float64
	spread          float64
}

func newSingleZoneShard(zone string, instanceByToken map[uint32]instanceInfo) singleZoneShard {
	return singleZoneShard{
		zone:            zone,
		shard:           make(map[string]InstanceDesc),
		instanceByToken: instanceByToken,
		tokenOwnership:  make(map[string]float64),
		spread:          -1,
	}
}

func (s *singleZoneShard) len() int {
	return len(s.shard)
}

func (s *singleZoneShard) contains(instance InstanceDesc) bool {
	_, ok := s.shard[instance.Id]
	return ok
}

func (s *singleZoneShard) getTokenOwnership() map[string]float64 {
	if len(s.tokenOwnership) == 0 {
		s.tokenOwnership = s.calculateTokenOwnership()
	}
	return s.tokenOwnership
}

func (s *singleZoneShard) getSpread() float64 {
	if s.spread < 0 {
		s.spread = s.calculateSpread()
	}
	return s.spread
}

// calculateTokenOwnership calculates token ownership for all the instances of this singleZoneShard.
func (s *singleZoneShard) calculateTokenOwnership() map[string]float64 {
	tokenOwnershipByInstanceID := make(map[string]float64, len(s.shard))

	tokens := make([][]uint32, len(s.shard))
	for _, inst := range s.shard {
		tokens = append(tokens, inst.GetTokens())
	}
	s.tokens = MergeTokens(tokens)

	prev := len(s.tokens) - 1
	for tk, token := range s.tokens {
		ownership := float64(tokenDistance(s.tokens[prev], token))
		tokenOwnershipByInstanceID[s.instanceByToken[token].InstanceID] += ownership
		prev = tk
	}
	return tokenOwnershipByInstanceID
}

// calculateSpread calculates the spread of this singleZoneShard.
func (s *singleZoneShard) calculateSpread() float64 {
	tokenOwnershipByInstance := s.calculateTokenOwnership()

	min := math.MaxFloat64
	max := 0.0
	for _, ownership := range tokenOwnershipByInstance {
		min = math.Min(ownership, min)
		max = math.Max(ownership, max)
	}
	return 1 - min/max
}

// deepCopy creates and returns a deep copy of this singleZoneShard.
func (s *singleZoneShard) deepCopy() singleZoneShard {
	shard := make(map[string]InstanceDesc, s.len())
	for id, instDesc := range s.shard {
		shard[id] = instDesc
	}
	tokens := make(Tokens, len(s.tokens))
	copy(tokens, s.tokens)

	return singleZoneShard{
		zone:            s.zone,
		shard:           shard,
		tokens:          tokens,
		instanceByToken: s.instanceByToken,
	}
}

// addInstance adds the given instance to this singeZoneShard.
// If the instance is not from the same zone as the shard, or if the instance is already present in the shard,
// an error is returned.
func (s *singleZoneShard) addInstance(instance InstanceDesc) error {
	if instance.Zone != s.zone {
		return ErrInconsistentInstanceZoneInfo
	}

	if _, ok := s.shard[instance.Id]; ok {
		// Instance is already present in the shard.
		return errSingleZoneShardInstanceAlreadyPresent(instance.Id)
	}
	s.shard[instance.Id] = instance
	tokens := [][]uint32{s.tokens, instance.Tokens}
	s.tokens = MergeTokens(tokens)
	s.tokenOwnership = make(map[string]float64, len(s.shard))
	s.spread = -1
	return nil
}

// add adds the given instance to this singeZoneShard.
// If the instance is not from the same zone as the shard,
// or if the instance is already present in the shard,
// an error is returned.
// This operation clears all token ownership and spread
// information contained in this shard.
func (s *singleZoneShard) add(instance InstanceDesc, tOwnership *tokenOwnership) ([]*list.Element, *tokenOwnership, error) {
	if instance.Zone != s.zone {
		return nil, nil, ErrInconsistentInstanceZoneInfo
	}

	if _, ok := s.shard[instance.Id]; ok {
		// Instance is already present in the shard.
		return nil, nil, errSingleZoneShardInstanceAlreadyPresent(instance.Id)
	}

	s.shard[instance.Id] = instance
	newTokenListElements := tOwnership.addTokens(instance, s.instanceByToken)
	s.updateSpread(tOwnership)

	/*for curr := tOwnership.tokenList.Front(); curr != nil; curr = curr.Next() {
		token := curr.Value.(uint32)
		instance := s.instanceByToken[token].InstanceID
		fmt.Printf("token: %20d, instance: %30s\n", token, instance)
	}
	fmt.Println(strings.Repeat("-", 50))
	for instanceID, ownership := range tOwnership.tokenOwnershipByInstance {
		fmt.Printf("instance: %30s, ownership: %.3f\n", instanceID, ownership)
	}
	fmt.Printf("ADDING INSTANCE %s COMPLETED. NEW SPREAD: %.3f\n", instance.Id, s.spread)

	to := s.calculateTokenOwnership()
	fmt.Println(to)*/

	return newTokenListElements, tOwnership, nil
}

func (s *singleZoneShard) remove(instance InstanceDesc, tOwnership *tokenOwnership, tokenListElements []*list.Element) (*tokenOwnership, error) {
	if _, ok := s.shard[instance.Id]; !ok {
		// Instance is already present in the shard.
		return nil, errSingleZoneShardInstanceNotPresent(instance.Id)
	}

	if instance.Zone != s.zone {
		return nil, ErrInconsistentInstanceZoneInfo
	}

	delete(s.shard, instance.Id)

	tOwnership.removeTokens(tokenListElements, s.instanceByToken)

	delete(tOwnership.tokenOwnershipByInstance, instance.Id)
	if len(tOwnership.tokenOwnershipByInstance) == 1 {
		s.spread = 0
	} else {
		s.spread = -1
	}
	s.updateSpread(tOwnership)

	/*for curr := tOwnership.tokenList.Front(); curr != tOwnership.tokenList.Back(); curr = curr.Next() {
		token := curr.Value.(uint32)
		instance := s.instanceByToken[token].InstanceID
		fmt.Printf("token: %20d, instance: %30s\n", token, instance)
	}
	fmt.Println(strings.Repeat("-", 50))
	for instanceID, ownership := range tOwnership.tokenOwnershipByInstance {
		fmt.Printf("instance: %30s, ownership: %.3f\n", instanceID, ownership)
	}
	fmt.Printf("REMOVING INSTANCE %s COMPLETED. NEW SPREAD: %.3f\n", instance.Id, s.spread)*/

	return tOwnership, nil
}

func (s *singleZoneShard) calculateTokenOwnershipWithCandidate(candidate *InstanceDesc) map[string]float64 {
	var (
		tokenOwnershipByInstanceID map[string]float64
		tokens                     Tokens
	)
	if candidate == nil {
		tokens = s.tokens
		tokenOwnershipByInstanceID = make(map[string]float64, len(s.shard))
	} else {
		tokens = MergeTokens([][]uint32{s.tokens, candidate.GetTokens()})
		tokenOwnershipByInstanceID = make(map[string]float64, len(s.shard)+1)
	}

	prev := len(tokens) - 1
	for tk, token := range tokens {
		ownership := float64(tokenDistance(tokens[prev], token))
		tokenOwnershipByInstanceID[s.instanceByToken[token].InstanceID] += ownership
		prev = tk
	}
	return tokenOwnershipByInstanceID
}

func (s *singleZoneShard) calculateSpreadWithCandidate(candidateInstance *InstanceDesc) float64 {
	if len(s.shard) == 0 {
		return 0
	}

	tokenOwnershipByInstance := s.calculateTokenOwnershipWithCandidate(candidateInstance)

	min := math.MaxFloat64
	max := 0.0
	for _, ownership := range tokenOwnershipByInstance {
		min = math.Min(ownership, min)
		max = math.Max(ownership, max)
	}
	return 1 - min/max
}

func (s *singleZoneShard) copy() singleZoneShard {
	shard := make(map[string]InstanceDesc, s.len()+1)
	for id, instDesc := range s.shard {
		shard[id] = instDesc
	}

	return singleZoneShard{
		zone:            s.zone,
		shard:           shard,
		tokens:          nil,
		tokenOwnership:  nil,
		spread:          s.spread,
		instanceByToken: s.instanceByToken,
	}
}

func (s *singleZoneShard) updateSpread(tOwnership *tokenOwnership) {
	if tOwnership.tokenList.Len() == 0 {
		return
	}

	if len(tOwnership.tokenOwnershipByInstance) == 1 {
		s.spread = 0
		return
	}

	min := math.MaxFloat64
	max := 0.0
	for _, ownership := range tOwnership.tokenOwnershipByInstance {
		min = math.Min(ownership, min)
		max = math.Max(ownership, max)
	}
	s.spread = 1 - min/max
}

type tokenOwnership struct {
	tokenList                *list.List
	tokenOwnershipByInstance map[string]float64
}

func newTokenOwnership(size int) *tokenOwnership {
	return &tokenOwnership{
		tokenList:                list.New(),
		tokenOwnershipByInstance: make(map[string]float64, size),
	}
}

func (t *tokenOwnership) addTokens(instance InstanceDesc, instanceByToken map[uint32]instanceInfo) []*list.Element {
	instanceTokens := instance.GetTokens()
	addedTokenListElements := make([]*list.Element, 0, len(instanceTokens))
	head := t.tokenList.Front()
	if head == nil {
		for _, token := range instanceTokens {
			newElement := t.tokenList.PushBack(token)
			addedTokenListElements = append(addedTokenListElements, newElement)
		}
		t.tokenOwnershipByInstance[instance.Id] = totalTokensCount
		return addedTokenListElements
	}

	currElem := head
	currIdx := 0
	for currElem != nil && currIdx < len(instanceTokens) {
		if currElem.Value.(uint32) <= instanceTokens[currIdx] {
			currElem = currElem.Next()
			continue
		}
		// At this point currElem is the first element greater than instanceTokens[currIdx].
		newElement := t.tokenList.InsertBefore(instanceTokens[currIdx], currElem)
		addedTokenListElements = append(addedTokenListElements, newElement)
		currIdx++
	}
	for ; currIdx < len(instanceTokens); currIdx++ {
		newElement := t.tokenList.PushBack(instanceTokens[currIdx])
		addedTokenListElements = append(addedTokenListElements, newElement)
	}

	t.updateOwnership(instanceByToken)
	return addedTokenListElements
}

func (t *tokenOwnership) removeTokens(tokenListElements []*list.Element, instanceByToken map[uint32]instanceInfo) {
	for _, tokenListElement := range tokenListElements {
		t.tokenList.Remove(tokenListElement)
	}
	t.updateOwnership(instanceByToken)
}

func (t *tokenOwnership) updateOwnership(instanceByToken map[uint32]instanceInfo) {
	if t.tokenList.Len() == 0 {
		return
	}

	for instanceID := range t.tokenOwnershipByInstance {
		t.tokenOwnershipByInstance[instanceID] = 0
	}

	for prev, tok := t.tokenList.Back(), t.tokenList.Front(); tok != t.tokenList.Back(); prev, tok = tok, tok.Next() {
		from := prev.Value.(uint32)
		to := tok.Value.(uint32)
		ownership := float64(tokenDistance(from, to))
		t.tokenOwnershipByInstance[instanceByToken[to].InstanceID] += ownership
	}
}

func (t *tokenOwnership) ownershipUpdate(newTokenListElements []*list.Element, instanceByToken map[uint32]instanceInfo, addingTokens bool) {
	if t.tokenList.Len() == 0 || len(newTokenListElements) == 0 {
		return
	}

	firstTokenListElement := newTokenListElements[0]
	firstToken := firstTokenListElement.Value.(uint32)
	currInstanceID := instanceByToken[firstToken].InstanceID
	// All tokens from newTokenListElements are owned by the same instance, currInstanceID.

	// We are first looking for the first left border, i.e., a token from t.tokenList
	// that precedes a group of possibly consecutive tokens containing firstToken.
	// The range of tokens between the first left border and the last token of that
	// group of tokens is owned by the instance currentInstanceID.

	// We consider the direct predecessor of firstTokenListElement in the list of tokens t.tokenList.
	// If it is nil, it means that firstTokenListElement is the head of t.tokenList, and we proceed
	// searching for the first left border from the tail of t.tokenList.
	firstLeftBorder := firstTokenListElement.Prev()
	if firstLeftBorder == nil {
		firstLeftBorder = t.tokenList.Back()
	}
	lastIdx := len(newTokenListElements) - 1
	for lastIdx >= 0 {
		if newTokenListElements[lastIdx] != firstLeftBorder {
			break
		}
		lastIdx--
		firstLeftBorder = firstLeftBorder.Prev()
	}

	if lastIdx == 0 {
		// If we reach this point, the current list of tokens in t.tokenList contains only tokens owned
		// by the instance currentInstanceID. In this case that instance owns the whole set of tokens.
		t.tokenOwnershipByInstance[currInstanceID] = totalTokensCount
		return
	}

	// At this point firstLeftBorder is the first left border. We will now look for pairs of left borders
	// and right borders and update instance token ownerships. All tokens from a left border to the token
	// from newTokenListElements directly preceding a right border will be owned by the instance
	// currentInstanceID.

	leftBorder := firstLeftBorder

	idx := 0
	for idx <= lastIdx {
		rightIdx := idx
		for rightIdx+1 <= lastIdx {
			if newTokenListElements[rightIdx].Next() != newTokenListElements[rightIdx+1] {
				break
			}
			rightIdx++
		}

		// At this point newTokenListElements[rightIdx] has a successor in the current list of tokens,
		// and that successor is not owned by currentInstanceID.
		nextToken := newTokenListElements[rightIdx].Next().Value.(uint32)
		nextInstanceID := instanceByToken[nextToken].InstanceID

		// All the tokens between leftBorder and newTokenListElements[rightIdx] can be assigned to
		// the instance currentInstanceID, and removed from the instance nextInstanceID.
		from := leftBorder.Value.(uint32)
		to := newTokenListElements[rightIdx].Value.(uint32)
		ownershipDelta := float64(tokenDistance(from, to))
		if addingTokens {
			t.tokenOwnershipByInstance[currInstanceID] += ownershipDelta
			t.tokenOwnershipByInstance[nextInstanceID] -= ownershipDelta
		} else {
			t.tokenOwnershipByInstance[currInstanceID] -= ownershipDelta
			t.tokenOwnershipByInstance[nextInstanceID] += ownershipDelta
		}

		leftBorder = newTokenListElements[rightIdx].Next()
		idx = rightIdx + 1
	}
}
