package ring

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

var (
	zones             = []string{"zone-a", "zone-b", "zone-c"}
	tokensPerInstance = 512
)

func TestSpreadMinimizingTokenGenerator_GetInstanceID(t *testing.T) {
	tests := map[string]struct {
		instanceID    string
		expectedID    int
		expectedError error
	}{
		"instance-zone-a-10 is correct": {
			instanceID: "instance-zone-a-10",
			expectedID: 10,
		},
		"instance-zone-b-0 is correct": {
			instanceID: "instance-zone-b-0",
			expectedID: 0,
		},
		"instance-zone-5 is not valid": {
			instanceID:    "instance-zone-5",
			expectedError: errorBadInstanceIDFormat("instance-zone-5"),
		},
		"instance-zone-c is not valid": {
			instanceID:    "instance-zone-c",
			expectedError: errorBadInstanceIDFormat("instance-zone-c"),
		},
		"empty instance is not valid": {
			instanceID:    "",
			expectedError: errorBadInstanceIDFormat(""),
		},
	}
	for _, testData := range tests {
		ID, err := getInstanceID(testData.instanceID)
		if testData.expectedError != nil {
			require.Error(t, err)
			require.Equal(t, testData.expectedError, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, testData.expectedID, ID)
		}
	}
}

func TestSpreadMinimizingTokenGenerator_GetZoneID(t *testing.T) {
	tests := map[string]struct {
		zone          string
		zones         []string
		expectedID    int
		expectedZones []string
		expectedError error
	}{
		"zone-a has index 0": {
			zone:       "zone-a",
			zones:      []string{"zone-a", "zone-b", "zone-c"},
			expectedID: 0,
		},
		"zone-c has index 2 in a sorted slice of zones": {
			zone:       "zone-c",
			zones:      []string{"zone-a", "zone-b", "zone-c"},
			expectedID: 2,
		},
		"zone-c has index 2 in an unsorted slice of zones": {
			zone:          "zone-c",
			zones:         []string{"zone-c", "zone-b", "zone-a"},
			expectedID:    2,
			expectedZones: []string{"zone-a", "zone-b", "zone-c"},
		},
		"zone-d is not valid": {
			zone:          "zone-d",
			zones:         []string{"zone-a", "zone-b", "zone-c"},
			expectedError: errorZoneNotValid("zone-d"),
		},
	}
	for _, testData := range tests {
		zoneID, err := getZoneID(testData.zone, testData.zones)
		if testData.expectedError != nil {
			require.Error(t, err)
			require.Equal(t, testData.expectedError, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, testData.expectedID, zoneID)
			if testData.expectedZones != nil {
				require.Equal(t, testData.expectedZones, testData.zones)
			}
		}
	}
}

func TestSpreadMinimizingTokenGenerator_GenerateFirstInstanceTokens(t *testing.T) {
	zonesCount := len(zones)
	for z, zone := range zones {
		instanceID := fmt.Sprintf("instance-%s-%d", zone, 10)
		cfg := &SpreadMinimizingConfig{instanceID, zone, tokensPerInstance}
		tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, zones)
		tokens := tokenGenerator.generateFirstInstanceTokens()
		for i, token := range tokens {
			require.Equal(t, uint32(1<<23/zonesCount*zonesCount*i+z), token)
			require.True(t, token%uint32(zonesCount) == uint32(z))
		}
	}
}

func TestSpreadMinimizingTokenGenerator_GenerateFirstInstanceTokensIdempotent(t *testing.T) {
	for _, zone := range zones {
		instanceID := fmt.Sprintf("instance-%s-%d", zone, 10)
		cfg := &SpreadMinimizingConfig{instanceID, zone, tokensPerInstance}
		tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, zones)
		tokens1 := tokenGenerator.generateFirstInstanceTokens()
		require.Len(t, tokens1, tokensPerInstance)
		tokens2 := tokenGenerator.generateFirstInstanceTokens()
		require.True(t, tokens1.Equals(tokens2))
	}
}

func TestSpreadMinimizingTokenGenerator_GetOptimalTokenOwnership(t *testing.T) {
	tests := []struct {
		optimalInstanceOwnership      float64
		currInstanceOwnership         float64
		currTokensCount               uint32
		expectedOptimalTokenOwnership uint32
	}{
		{
			optimalInstanceOwnership:      1000.00,
			currInstanceOwnership:         900.00,
			currTokensCount:               4,
			expectedOptimalTokenOwnership: 24,
		},
		{
			optimalInstanceOwnership:      1000.00,
			currInstanceOwnership:         990.00,
			currTokensCount:               4,
			expectedOptimalTokenOwnership: 0,
		},
	}
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, nil, zones)
	for _, testData := range tests {
		optimalTokenOwnership := tokenGenerator.getOptimalTokenOwnership(testData.optimalInstanceOwnership, testData.currInstanceOwnership, testData.currTokensCount)
		require.Equal(t, testData.expectedOptimalTokenOwnership, optimalTokenOwnership)
	}
}

func TestSpreadMinimizingTokenGenerator_CalculateNewToken(t *testing.T) {
	tests := map[string]struct {
		ringToken             ringToken
		optimalTokenOwnership uint32
		expectedNewToken      uint32
		expectedError         error
	}{
		"zoneID 0, prevToken < token": {
			ringToken:             ringToken{90, 30},
			optimalTokenOwnership: 30,
			expectedNewToken:      60,
		},
		"zoneID 1, prevToken < token": {
			ringToken:             ringToken{91, 31},
			optimalTokenOwnership: 30,
			expectedNewToken:      61,
		},
		"zoneID 2, prevToken < token": {
			ringToken:             ringToken{92, 32},
			optimalTokenOwnership: 30,
			expectedNewToken:      62,
		},
		"zoneID 0, prevToken > token": {
			ringToken:             ringToken{420, 4294967142},
			optimalTokenOwnership: 210,
			expectedNewToken:      60,
		},
		"zoneID 1, prevToken > token": {
			ringToken:             ringToken{421, 4294967143},
			optimalTokenOwnership: 210,
			expectedNewToken:      61,
		},
		"zoneID 2, prevToken > token": {
			ringToken:             ringToken{422, 4294967144},
			optimalTokenOwnership: 210,
			expectedNewToken:      62,
		},
		"zoneID 0, prevToken > token, offset > optimalTokenOwnership": {
			ringToken:             ringToken{420, 4294967142},
			optimalTokenOwnership: 120,
			expectedNewToken:      4294967262,
		},
		"zoneID 1, prevToken > token, offset > optimalTokenOwnership": {
			ringToken:             ringToken{421, 4294967143},
			optimalTokenOwnership: 120,
			expectedNewToken:      4294967263,
		},
		"zoneID 2, prevToken > token, offset > optimalTokenOwnership": {
			ringToken:             ringToken{422, 4294967144},
			optimalTokenOwnership: 120,
			expectedNewToken:      4294967264,
		},
		"bad congruence": {
			ringToken:             ringToken{90, 31},
			optimalTokenOwnership: 30,
			expectedError:         fmt.Errorf("calculation of a new token between 31 and 90 with optimal token ownership 30 was impossible: lower and upper bounds must be congruent modulo number of zones 3"),
		},
		"optimalTokenOwnership small": {
			ringToken:             ringToken{90, 30},
			optimalTokenOwnership: 2,
			expectedError:         fmt.Errorf("calculation of a new token between 30 and 90 with optimal token ownership 2 was impossible: optimal token ownership must be a positive multiple of number of zones 3"),
		},
		"optimalTokenOwnership bad congruence": {
			ringToken:             ringToken{90, 30},
			optimalTokenOwnership: 32,
			expectedError:         fmt.Errorf("calculation of a new token between 30 and 90 with optimal token ownership 32 was impossible: optimal token ownership must be a positive multiple of number of zones 3"),
		},
		"optimalTokenOwnership too big": {
			ringToken:             ringToken{90, 30},
			optimalTokenOwnership: 300,
			expectedError:         fmt.Errorf("calculation of a new token between 30 and 90 with optimal token ownership 300 was impossible: distance between lower and upper bound 60 is not big enough"),
		},
	}
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, nil, zones)
	for _, testData := range tests {
		newToken, err := tokenGenerator.calculateNewToken(testData.ringToken, testData.optimalTokenOwnership)
		if testData.expectedError == nil {
			require.NoError(t, err)
			require.Equal(t, testData.expectedNewToken, newToken)
		} else {
			require.Error(t, err)
			require.Equal(t, testData.expectedError, err)
		}
	}
}

func TestSpreadMinimizingTokenGenerator_GenerateAllTokensIdempotent(t *testing.T) {
	maxInstanceID := 128
	for instanceID := 0; instanceID < maxInstanceID; instanceID++ {
		for _, zone := range zones {
			instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
			cfg := &SpreadMinimizingConfig{instance, zone, tokensPerInstance}
			tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, zones)
			tokens1 := tokenGenerator.generateAllTokens()
			require.Len(t, tokens1, tokensPerInstance)
			tokens2 := tokenGenerator.generateAllTokens()
			require.True(t, tokens1.Equals(tokens2))
		}
	}
}

func TestSpreadMinimizingTokenGenerator_VerifyTokensByZone(t *testing.T) {
	tokensPerInstance := 512
	instancesPerZone := 128
	_, tokensByZone := createTokensForAllInstancesAndZones(t, instancesPerZone, tokensPerInstance)
	for i := 0; i < instancesPerZone*tokensPerInstance; i++ {
		for z := 1; z < len(zones); z++ {
			tokenPrevZone := tokensByZone[zones[z-1]][i]
			tokenCurrZone := tokensByZone[zones[z]][i]
			require.Equal(t, tokenPrevZone+1, tokenCurrZone)
		}
	}
}

func TestSpreadMinimizingTokenGenerator_VerifyInstanceOwnershipSpreadByZone(t *testing.T) {
	tokensPerInstance := 512
	instancesPerZone := 10000
	instanceByToken, tokensByZone := createTokensForAllInstancesAndZones(t, instancesPerZone, tokensPerInstance)
	ownershipByInstanceByZone := getRegisteredOwnershipByZone(instancesPerZone, instanceByToken, tokensByZone)
	for _, ownershipByInstance := range ownershipByInstanceByZone {
		own := 0.0
		minOwnership := math.MaxFloat64
		maxOwnership := 0.0
		for _, ownership := range ownershipByInstance {
			own += ownership
			minOwnership = math.Min(minOwnership, ownership)
			maxOwnership = math.Max(maxOwnership, ownership)
		}
		spread := 100 * (1.0 - minOwnership/maxOwnership)
		require.Less(t, spread, 0.2)
	}
}

func TestSpreadMinimizingTokenGenerator_CheckTokenUniqueness(t *testing.T) {
	tokensPerInstance := 512
	instanceID := 10000
	allTokens := make(map[uint32]bool, tokensPerInstance*(instanceID+1)*len(zones))
	for _, zone := range zones {
		instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
		cfg := &SpreadMinimizingConfig{instance, zone, tokensPerInstance}
		tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, zones)
		tokens := tokenGenerator.generateTokensByInstanceID()
		for i := 0; i <= instanceID; i++ {
			tks := tokens[i]
			for _, token := range tks {
				if _, found := allTokens[token]; found {
					err := fmt.Errorf("token %d been found more than once", token)
					panic(err)
				}
				allTokens[token] = true
			}
		}
	}
}

func TestSpreadMinimizingTokenGenerator_GenerateTokens(t *testing.T) {
	tokensPerInstance := 512
	instanceID := 1000
	zone := zones[0]
	instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
	cfg := &SpreadMinimizingConfig{instance, zone, tokensPerInstance}
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, zones)
	// this is the set of all sorted tokens assigned to instance
	allTokens := tokenGenerator.generateAllTokens()
	require.Len(t, allTokens, tokensPerInstance)

	// we try to generate 1024, and we ensure that only allTokens will be returned
	tokens := tokenGenerator.GenerateTokens(2*tokensPerInstance, nil)
	require.Len(t, tokens, tokensPerInstance)
	require.True(t, allTokens.Equals(tokens))

	takenTokens := make(Tokens, 0, tokensPerInstance)
	tokensCount := 300
	// we get the first tokensCount tokens and ensure that they are returned in the same order
	// they have in allTokens
	tokens = tokenGenerator.GenerateTokens(tokensCount, takenTokens)
	require.Len(t, tokens, tokensCount)
	for i := 0; i < tokensCount; i++ {
		require.Equal(t, allTokens[i], tokens[i])
	}
	// we mark the returned tokens as taken
	takenTokens = append(takenTokens, tokens...)

	// we get the remaining tokens and ensure that they are actually the remaining tokens from allTokens
	// returned in the same order
	remainingTokensCount := tokensPerInstance - tokensCount
	remainingTokens := tokenGenerator.GenerateTokens(remainingTokensCount, takenTokens)
	require.Len(t, remainingTokens, remainingTokensCount)
	for i := 0; i < remainingTokensCount; i++ {
		require.Equal(t, allTokens[i+tokensCount], remainingTokens[i])
	}

	// we mark remaining tokens as taken
	takenTokens = append(takenTokens, remainingTokens...)

	// we ensure that further attempts to generate tokens return nothing
	noTokens := tokenGenerator.GenerateTokens(tokensPerInstance, takenTokens)
	require.Len(t, noTokens, 0)
}

func TestSpreadMinimizingTokenGenerator_GetMissingTokens(t *testing.T) {
	tokensPerInstance := 512
	instanceID := 1000
	zone := zones[0]
	instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
	cfg := &SpreadMinimizingConfig{instance, zone, tokensPerInstance}
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, zones)

	// we get all the tokens for the underlying instance, but we don't mark all of them as taken
	// in order to simulate that some tokens were taken by another instance when the method was
	// first called
	missingIndexes := []int{rand.Intn(tokensPerInstance - 1), rand.Intn(tokensPerInstance - 1), rand.Intn(tokensPerInstance - 1)}
	slices.Sort(missingIndexes)
	takenTokens := make(Tokens, 0, tokensPerInstance)
	allTokens := tokenGenerator.GenerateTokens(tokensPerInstance, takenTokens)

	for i, token := range allTokens {
		if slices.Contains(missingIndexes, i) {
			continue
		}
		takenTokens = append(takenTokens, token)
	}

	// we generate the missing tokens, and we ensure that they correspond to the
	// tokens of allTokens having indexes in missingIndexes.
	tokens := tokenGenerator.GenerateTokens(len(missingIndexes), takenTokens)
	require.Len(t, tokens, len(missingIndexes))
	for i, missingIndex := range missingIndexes {
		require.Equal(t, allTokens[missingIndex], tokens[i])
	}
}

func createTokensForAllInstancesAndZones(t *testing.T, maxInstanceID, tokensPerInstance int) (map[uint32]*instanceInfo, map[string][]uint32) {
	instanceByToken := make(map[uint32]*instanceInfo, (maxInstanceID+1)*tokensPerInstance*len(zones))
	tokenSetsByZone := make(map[string][][]uint32, len(zones))
	for _, zone := range zones {
		finalInstance := fmt.Sprintf("instance-%s-%d", zone, maxInstanceID)
		cfg := &SpreadMinimizingConfig{finalInstance, zone, tokensPerInstance}
		tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, zones)
		tokensByInstance := tokenGenerator.generateTokensByInstanceID()
		for id, tokens := range tokensByInstance {
			instInfo := &instanceInfo{
				InstanceID: fmt.Sprintf("instance-%s-%d", zone, id),
				Zone:       zone,
			}
			for _, token := range tokens {
				if inst, ok := instanceByToken[token]; ok {
					err := fmt.Errorf("token %d was already assigned to instance %s", token, inst.InstanceID)
					panic(err)
				}
				instanceByToken[token] = instInfo
			}
			allTokens, ok := tokenSetsByZone[zone]
			if !ok {
				allTokens = make([][]uint32, 0, maxInstanceID+1)
			}
			allTokens = append(allTokens, tokens)
			tokenSetsByZone[zone] = allTokens
		}
	}

	tokensByZone := MergeTokensByZone(tokenSetsByZone)
	return instanceByToken, tokensByZone
}

func createSpreadMinimizingTokenGenerator(t *testing.T, cfg *SpreadMinimizingConfig, zones []string) *SpreadMinimizingTokenGenerator {
	if cfg == nil {
		cfg = &SpreadMinimizingConfig{"instance-zone-a-10", "zone-a", tokensPerInstance}
	}
	tokenGenerator, err := NewSpreadMinimizingTokenGenerator(*cfg, zones, log.NewLogfmtLogger(os.Stdout))
	require.NoError(t, err)
	require.NotNil(t, tokenGenerator)
	return tokenGenerator
}

// getRegisteredOwnershipByZone calculates ownership maps grouped by instance id and by zone
func getRegisteredOwnershipByZone(instancesPerZone int, instanceByToken map[uint32]*instanceInfo, tokensByZone map[string][]uint32) map[string]map[string]float64 {
	ownershipByInstanceByZone := make(map[string]map[string]float64, len(zones))
	for zone, tokens := range tokensByZone {
		ownershipByInstanceByZone[zone] = make(map[string]float64, instancesPerZone)
		if len(tokens) == 0 {
			continue
		}
		prev := len(tokens) - 1
		for tk, token := range tokens {
			ownership := float64(getTokenDistance(tokens[prev], token))
			ownershipByInstanceByZone[zone][instanceByToken[token].InstanceID] += ownership
			prev = tk
		}
	}
	return ownershipByInstanceByZone
}

// generateTokensByInstanceID is a modified implementation of SpreadMinimizingTokenGenerator.generateAllTokens that
// is used in the tests to speed up the generation of tokens, since the generation of tokens for instance with id
// instanceID already generates the tokens of all other instances with lower ids.
func (t *SpreadMinimizingTokenGenerator) generateTokensByInstanceID() map[int]Tokens {
	firstInstanceTokens := t.generateFirstInstanceTokens()

	if t.instanceID == 0 {
		return map[int]Tokens{0: firstInstanceTokens}
	}

	tokensCount := t.cfg.TokensPerInstance

	// tokensQueues is a slice of priority queues. Slice indexes correspond
	// to the ids of instances, while priority queues represent the tokens
	// of the corresponding instance, ordered from highest to lowest ownership.
	tokensQueues := make([]ownershipPriorityQueue[ringToken], t.instanceID)

	// Create and initialize priority queue of tokens for the first instance
	tokensQueue := newPriorityQueue[ringToken](tokensCount)
	prev := len(firstInstanceTokens) - 1
	firstInstanceOwnership := 0.0
	for tk, token := range firstInstanceTokens {
		tokenOwnership := float64(getTokenDistance(firstInstanceTokens[prev], token))
		firstInstanceOwnership += tokenOwnership
		tokensQueue.Add(newRingTokenOwnershipInfo(token, firstInstanceTokens[prev]))
		prev = tk
	}
	heap.Init(&tokensQueue)
	tokensQueues[0] = tokensQueue

	// instanceQueue is a priority queue of instances such that instances with higher ownership have a higher priority
	instanceQueue := newPriorityQueue[ringInstance](t.instanceID)
	instanceQueue.Add(newRingInstanceOwnershipInfo(0, firstInstanceOwnership))
	heap.Init(&instanceQueue)

	allTokens := make(map[int]Tokens, t.instanceID+1)
	allTokens[0] = firstInstanceTokens

	for i := 1; i <= t.instanceID; i++ {
		optimalInstanceOwnership := float64(totalTokensCount) / float64(i+1)
		currInstanceOwnership := 0.0
		addedTokens := 0
		tokens := make(Tokens, 0, tokensCount)
		// currInstanceTokenQueue is the priority queue of tokens of newInstance
		currInstanceTokenQueue := newPriorityQueue[ringToken](tokensCount)
		ignoredInstances := make([]ownershipInfo[ringInstance], 0, t.instanceID)
		for addedTokens < tokensCount {
			optimalTokenOwnership := t.getOptimalTokenOwnership(optimalInstanceOwnership, currInstanceOwnership, uint32(tokensCount-addedTokens))
			highestOwnershipInstance := instanceQueue.Peek()
			if highestOwnershipInstance.ownership <= float64(optimalTokenOwnership) {
				level.Error(t.logger).Log("msg", "it was impossible to add a token because the instance with the highest ownership cannot satisfy the request", "added tokens", addedTokens+1, "highest ownership", highestOwnershipInstance.ownership, "requested ownership", optimalTokenOwnership)
				break
			}
			tokensQueue := tokensQueues[highestOwnershipInstance.item.instanceID]
			highestOwnershipToken := tokensQueue.Peek()
			if highestOwnershipToken.ownership <= float64(optimalTokenOwnership) {
				// token with the highest ownership of the instance with the highest ownership
				// could not satisfy the request, hence we pass to the next instance.
				ignoredInstances = append(ignoredInstances, heap.Pop(&instanceQueue).(ownershipInfo[ringInstance]))
				continue
			}
			token := highestOwnershipToken.item
			newToken, err := t.calculateNewToken(token, optimalTokenOwnership)
			if err != nil {
				return nil
			}
			tokens = append(tokens, newToken)
			// add the new token to currInstanceTokenQueue
			currInstanceTokenQueue.Add(newRingTokenOwnershipInfo(newToken, token.prevToken))

			oldTokenOwnership := highestOwnershipToken.ownership
			newTokenOwnership := float64(getTokenDistance(newToken, token.token))
			currInstanceOwnership += oldTokenOwnership - newTokenOwnership

			highestOwnershipToken.item.prevToken = newToken
			highestOwnershipToken.ownership = newTokenOwnership
			heap.Fix(&tokensQueue, 0)

			highestOwnershipInstance.ownership = highestOwnershipInstance.ownership - oldTokenOwnership + newTokenOwnership
			heap.Fix(&instanceQueue, 0)

			addedTokens++
		}
		slices.Sort(tokens)
		allTokens[i] = tokens
		if i == t.instanceID {
			return allTokens
		}
		if len(ignoredInstances) != 0 {
			for _, ignoredInstance := range ignoredInstances {
				heap.Push(&instanceQueue, ignoredInstance)
			}
		}
		heap.Init(&currInstanceTokenQueue)
		tokensQueues[i] = currInstanceTokenQueue

		// add the current instance with the calculated ownership currInstanceOwnership to instanceQueue
		heap.Push(&instanceQueue, newRingInstanceOwnershipInfo(i, currInstanceOwnership))
	}

	return nil
}
