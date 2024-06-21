package ring

import (
	"fmt"
	"math"
	"math/rand"
	"sort"

	shardUtil "github.com/grafana/dskit/ring/shard"
	"golang.org/x/exp/slices"
)

const (
	threshold = 0.2
)

type spreadMinimizingShuffleSharder struct {
	cachedFirstZoneShards map[string]map[int]map[string]InstanceDesc
}

func newSpreadMinimizingShuffleSharder() *spreadMinimizingShuffleSharder {
	return &spreadMinimizingShuffleSharder{
		cachedFirstZoneShards: map[string]map[int]map[string]InstanceDesc{},
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

	spreadMinimizingTokenGenerator, err := NewSpreadMinimizingTokenGenerator(info.InstanceID, info.Zone, actualZones, false)
	if err != nil {
		return nil
	}
	instanceID := info.InstanceID
	firstInstanceDesc := ringInstancesById[instanceID]
	firstInstanceTokens := firstInstanceDesc.GetTokens()

	selectedInstances := make(map[string]InstanceDesc, shardSizePerZone)
	selectedInstances[instanceID] = firstInstanceDesc

	currSelectedInstanceIdx := 0
	selectedTokensByInstanceID := make(map[int]Tokens, shardSizePerZone)
	selectedTokensByInstanceID[currSelectedInstanceIdx] = firstInstanceTokens
	allSelectedTokens := make(Tokens, len(firstInstanceTokens))
	copy(allSelectedTokens, firstInstanceTokens)

	currSize := 0
	if !isWithinLookbackPeriod(firstInstanceDesc.RegisteredTimestamp) {
		currSize++
	}

	for currSize < shardSizePerZone {
		currSelectedInstanceIdx++
		nextSetOfTokens, err := spreadMinimizingTokenGenerator.generateTokensByInstanceIDFromFirstNInstanceTokens(selectedTokensByInstanceID, currSelectedInstanceIdx)
		if err != nil {
			return nil
		}

		bestCandidateID := calculateSimilarityIndexes(nextSetOfTokens[currSelectedInstanceIdx], currSize, allSelectedTokens, tokens, selectedInstances, ringInstanceByToken, ringInstancesById)
		bestCandidateInstance, ok := ringInstancesById[bestCandidateID]
		if !ok {
			continue
		}
		if !isWithinLookbackPeriod(bestCandidateInstance.RegisteredTimestamp) {
			currSize++
		}
		selectedTokensByInstanceID[currSelectedInstanceIdx] = bestCandidateInstance.GetTokens()
		selectedInstances[bestCandidateID] = bestCandidateInstance
		allSelectedTokens = MergeTokens([][]uint32{allSelectedTokens, bestCandidateInstance.GetTokens()})

	}
	calculateOwnershipAndSpread(allSelectedTokens, ringInstanceByToken)
	return s.createRingDescFromTheFirstZoneShard(selectedInstances, actualZones, ringInstanceByToken, ringInstancesById)
}

// tokenOwnershipByZone calculates token ownership map grouped by instance id and by zone
func calculateOwnershipAndSpread(tokens Tokens, instanceByToken map[uint32]instanceInfo) {
	ownershipByInstance := make(map[string]float64, len(instanceByToken))
	prev := len(tokens) - 1
	for tk, token := range tokens {
		ownership := float64(tokenDistance(tokens[prev], token))
		instanceID := instanceByToken[token].InstanceID
		ownershipByInstance[instanceID] += ownership
		prev = tk
	}

	var (
		min = math.MaxFloat64
		max = 0.0
	)
	fmt.Print("\n\tCURRENT OWNERSHIP: [")
	for instanceID, ownership := range ownershipByInstance {
		min = math.Min(min, ownership)
		max = math.Max(max, ownership)
		fmt.Printf("instance: %30s - ownership: %10.3f", instanceID, ownership)
	}
	spread := 1 - min/max
	fmt.Printf("]\n\tSPREAD: %10.3f\n", spread)
	//fmt.Println(strings.Repeat("-", 50))
}

func calculateSimilarityIndexes(generatedTokens Tokens, instancePositionInShard int, allSelectedTokens Tokens, allTokensFromZone Tokens, selectedInstances map[string]InstanceDesc, ringInstanceByToken map[uint32]instanceInfo, ringInstancesById map[string]InstanceDesc) string {
	//optimalTokenDistance := totalTokensCount / optimalTokensPerInstance / instancePositionInShard
	precedingCandidateTokensByInstanceID := make(map[string]int, len(ringInstancesById))
	succeedingCandidateTokensByInstanceID := make(map[string]int, len(ringInstancesById))
	for _, generatedToken := range generatedTokens {
		closestPrecedingCandidateToken := selectClosestPrecedingTokenNotYetChosen(generatedToken, allTokensFromZone, selectedInstances, ringInstanceByToken)

		//closestSucceedingSelectedToken := allSelectedTokens[searchToken(allSelectedTokens, closestPrecedingCandidateToken)]
		//dist := tokenDistance(closestPrecedingCandidateToken, closesSucceedingSelectedToken)
		//fmt.Printf("the closest candidate token preceeding the generated token %d is %d, and it is at distance %d from its selected successor %d (optimal distance is %d)\n", generatedToken, closestPrecedingCandidateToken, dist, closesSucceedingSelectedToken, optimalTokenDistance)

		if instInfo, ok := ringInstanceByToken[closestPrecedingCandidateToken]; ok {
			precedingCandidateTokens, ok := precedingCandidateTokensByInstanceID[instInfo.InstanceID]
			if !ok {
				precedingCandidateTokens = 0
			}
			precedingCandidateTokensByInstanceID[instInfo.InstanceID] = precedingCandidateTokens + 1
		}

		closestSucceedingCandidateToken := selectClosestSucceedingTokenNotYetChosen(generatedToken, allTokensFromZone, selectedInstances, ringInstanceByToken)
		//closestSucceedingSelectedToken = allSelectedTokens[searchToken(allSelectedTokens, closestSucceedingCandidateToken)]
		//dist = tokenDistance(closestSucceedingCandidateToken, closesSucceedingSelectedToken)
		//fmt.Printf("\tthe closest candidate token succeeding the generated token %d is %d, and it is at distance %d from its selected successor %d (optimal distance is %d)\n", generatedToken, closestSucceedingCandidateToken, dist, closesSucceedingSelectedToken, optimalTokenDistance)

		if instInfo, ok := ringInstanceByToken[closestSucceedingCandidateToken]; ok {
			succeedingCandidateTokens, ok := succeedingCandidateTokensByInstanceID[instInfo.InstanceID]
			if !ok {
				succeedingCandidateTokens = 0
			}
			succeedingCandidateTokensByInstanceID[instInfo.InstanceID] = succeedingCandidateTokens + 1
		}
	}

	var (
		bestCandidate string
		highestCount  = 0
	)
	//fmt.Println("PRECEDING CANDIDATES BY INSTANCE IDS")
	for instanceID, count := range precedingCandidateTokensByInstanceID {
		//fmt.Printf("%30s: %10d\n", instanceID, timeseriesCount)
		if count > highestCount {
			highestCount = count
			bestCandidate = instanceID
		}
	}
	//fmt.Println(strings.Repeat("-", 50))
	/*fmt.Println("SUCCEEDING CANDIDATES BY INSTANCE IDS")
	for instanceID, timeseriesCount := range succeedingCandidateTokensByInstanceID {
		fmt.Printf("%30s: %10d\n", instanceID, timeseriesCount)
	}
	fmt.Println(strings.Repeat("-", 50))*/

	//fmt.Printf("Returning the best candidate with the highest number of preceding tokens: %s\n", bestCandidate)
	//fmt.Println(strings.Repeat("-", 50))
	return bestCandidate
}

type similarityInfo struct {
	instanceID  string
	occurrences int
}

type bySimilarity []similarityInfo

func (s bySimilarity) Len() int      { return len(s) }
func (s bySimilarity) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s bySimilarity) Less(i, j int) bool {
	if s[i].occurrences > s[j].occurrences {
		return true
	}
	if s[i].occurrences == s[j].occurrences {
		return s[i].instanceID < s[j].instanceID
	}
	return false
}

func getCandidatesSortedBySimilarity(generatedTokens Tokens, allTokensFromZone Tokens, selectedInstances map[string]InstanceDesc, ringInstanceByToken map[uint32]instanceInfo, ringInstancesByID map[string]InstanceDesc) bySimilarity {
	precedingCandidateTokensByInstanceID := make(map[string]int, len(ringInstancesByID))
	succeedingCandidateTokensByInstanceID := make(map[string]int, len(ringInstancesByID))
	for _, generatedToken := range generatedTokens {
		closestPrecedingCandidateToken := selectClosestPrecedingTokenNotYetChosen(generatedToken, allTokensFromZone, selectedInstances, ringInstanceByToken)

		if instInfo, ok := ringInstanceByToken[closestPrecedingCandidateToken]; ok {
			precedingCandidateTokens, ok := precedingCandidateTokensByInstanceID[instInfo.InstanceID]
			if !ok {
				precedingCandidateTokens = 0
			}
			precedingCandidateTokensByInstanceID[instInfo.InstanceID] = precedingCandidateTokens + 1
		}

		closestSucceedingCandidateToken := selectClosestSucceedingTokenNotYetChosen(generatedToken, allTokensFromZone, selectedInstances, ringInstanceByToken)

		if instInfo, ok := ringInstanceByToken[closestSucceedingCandidateToken]; ok {
			succeedingCandidateTokens, ok := succeedingCandidateTokensByInstanceID[instInfo.InstanceID]
			if !ok {
				succeedingCandidateTokens = 0
			}
			succeedingCandidateTokensByInstanceID[instInfo.InstanceID] = succeedingCandidateTokens + 1
		}
	}

	res := make(bySimilarity, 0, len(precedingCandidateTokensByInstanceID))
	for instanceID, count := range precedingCandidateTokensByInstanceID {
		res = append(res, similarityInfo{instanceID: instanceID, occurrences: count})
	}
	sort.Sort(res)
	return res
}

func selectClosestPrecedingTokenNotYetChosen(generatedToken uint32, allTokensFromZone Tokens, selectedInstances map[string]InstanceDesc, ringInstanceByToken map[uint32]instanceInfo) uint32 {
	var closestPrecedingToken uint32

	closestPrecedingTokenIdx := searchTokenPrev(allTokensFromZone, generatedToken)
	for offset := 0; offset < len(allTokensFromZone); offset++ {
		closestPrecedingTokenIdx -= offset
		if closestPrecedingTokenIdx < 0 {
			closestPrecedingTokenIdx = len(allTokensFromZone) - 1
		}
		closestPrecedingToken = allTokensFromZone[closestPrecedingTokenIdx]

		if instInfo, ok := ringInstanceByToken[closestPrecedingToken]; ok {
			if _, ok := selectedInstances[instInfo.InstanceID]; !ok {
				break
			}
		}
	}
	return closestPrecedingToken
}

func selectClosestSucceedingTokenNotYetChosen(generatedToken uint32, allTokensFromZone Tokens, selectedInstances map[string]InstanceDesc, ringInstanceByToken map[uint32]instanceInfo) uint32 {
	var closestSucceedingToken uint32
	closestSucceedingTokenIdx := searchToken(allTokensFromZone, generatedToken)

	for offset := 0; offset < len(allTokensFromZone); offset++ {
		closestSucceedingTokenIdx += offset
		if closestSucceedingTokenIdx > len(allTokensFromZone)-1 {
			closestSucceedingTokenIdx = 0
		}
		closestSucceedingToken = allTokensFromZone[closestSucceedingTokenIdx]

		if instInfo, ok := ringInstanceByToken[closestSucceedingToken]; ok {
			if _, ok := selectedInstances[instInfo.InstanceID]; !ok {
				break
			}
		}
	}
	return closestSucceedingToken
}

func searchTokenPrev(tokens []uint32, token uint32) int {
	i, found := slices.BinarySearch(tokens, token)

	if found {
		// In this case token was found in tokens, so we return its index.
		return i
	}

	// At this point token was not found, and i points to the position where it would be
	// placed if added to tokens keeping the ascending sorted order.
	// This means that tokens found at all indexes preceding i are lower than the searched token.
	// We need to retrun the highest among them, i.e., i - 1.
	// On the other hand, if is 0, we return the id of the last token, since that would be the
	// token preceding the searched one.
	if i == 0 {
		return len(tokens) - 1
	}
	return i - 1
}

func (s *spreadMinimizingShuffleSharder) createRingDescFromTheFirstZoneShard(selectedInstances map[string]InstanceDesc, sortedZones []string, ringInstanceByToken map[uint32]instanceInfo, ringInstancesById map[string]InstanceDesc) *Desc {
	shard := make(map[string]InstanceDesc, len(sortedZones)*len(selectedInstances))
	for _, instanceFromFirstZone := range selectedInstances {
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

func (s *spreadMinimizingShuffleSharder) shuffleShardNew(
	identifier string,
	actualZones []string,
	shardSizePerZone int,
	ringTokensByZone map[string][]uint32,
	ringInstanceByToken map[uint32]instanceInfo,
	ringInstancesByID map[string]InstanceDesc,
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

	spreadMinimizingTokenGenerator, err := NewSpreadMinimizingTokenGenerator(info.InstanceID, info.Zone, actualZones, false)
	if err != nil {
		return nil
	}
	instanceID := info.InstanceID
	firstInstanceDesc := ringInstancesByID[instanceID]
	firstInstanceTokens := firstInstanceDesc.GetTokens()

	selectedInstances := make(map[string]InstanceDesc, shardSizePerZone)
	selectedInstances[instanceID] = firstInstanceDesc

	maxSelectionSize := len(ringInstancesByID) / len(actualZones)

	tokensByID, err := spreadMinimizingTokenGenerator.generateTokensByInstanceIDFromTheFirstInstanceTokens(firstInstanceTokens, maxSelectionSize-1)
	if err != nil {
		return nil
	}

	allGeneratedTokensSets := make([][]uint32, 0, shardSizePerZone)
	for _, tokens := range tokensByID {
		allGeneratedTokensSets = append(allGeneratedTokensSets, tokens)
	}
	allGeneratedTokens := MergeTokens(allGeneratedTokensSets)

	currSize := 0
	if !isWithinLookbackPeriod(firstInstanceDesc.RegisteredTimestamp) {
		currSize++
	}

	sortedCandidates := getCandidatesSortedBySimilarity(allGeneratedTokens, firstInstanceTokens, selectedInstances, ringInstanceByToken, ringInstancesByID)
	for _, candidateID := range sortedCandidates {
		if currSize == shardSizePerZone {
			break
		}

		candidateInstanceDesc, ok := ringInstancesByID[candidateID.instanceID]
		if !ok {
			// Should never happen, but in this case we continue
			continue
		}
		selectedInstances[candidateID.instanceID] = candidateInstanceDesc
		if !isWithinLookbackPeriod(candidateInstanceDesc.RegisteredTimestamp) {
			currSize++
		}
	}
	calculateOwnershipAndSpread(allGeneratedTokens, ringInstanceByToken)
	return s.createRingDescFromTheFirstZoneShard(selectedInstances, actualZones, ringInstanceByToken, ringInstancesByID)
}
