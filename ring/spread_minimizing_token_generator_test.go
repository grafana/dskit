package ring

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/go-kit/log"

	"github.com/grafana/dskit/flagext"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

const (
	testInstance = "instance-zone-a-1000"
	testZone     = "zone-a"
)

var (
	zones             = []string{"zone-a", "zone-b", "zone-c"}
	tokensPerInstance = 512
	zone              = func(id int) string {
		return fmt.Sprintf("zone%d", id)
	}
)

func TestSpreadMinimizingConfig_Empty(t *testing.T) {
	var spreadMinimizingconfig SpreadMinimizingConfig
	flagext.DefaultValues(&spreadMinimizingconfig)
	require.True(t, spreadMinimizingconfig.Empty())

	spreadMinimizingconfig.SpreadMinimizingZones = zones
	require.False(t, spreadMinimizingconfig.Empty())
}

func TestSpreadMinimizingTokenGenerator_ParseInstanceID(t *testing.T) {
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
		"store-gateway-zone-c-7 is correct": {
			instanceID: "store-gateway-zone-c-7",
			expectedID: 7,
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
		id, err := parseInstanceID(testData.instanceID)
		if testData.expectedError != nil {
			require.Error(t, err)
			require.Equal(t, testData.expectedError, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, testData.expectedID, id)
		}
	}
}

func TestSpreadMinimizingTokenGenerator_FindZoneID(t *testing.T) {
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
		"zone-d is not valid": {
			zone:          "zone-d",
			zones:         []string{"zone-a", "zone-b", "zone-c"},
			expectedError: errorZoneNotValid("zone-d"),
		},
	}
	for _, testData := range tests {
		zoneID, err := findZoneID(testData.zone, testData.zones)
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

func TestSpreadMinimizingTokenGenerator_NewSpreadMinimizingTokenGenerator(t *testing.T) {
	tests := map[string]struct {
		spreadMinimizingZones []string
		instance              string
		zone                  string
		expectedError         error
	}{
		"if spreadMinimizingZones is empty, errorZoneCountOutOfBound is returned": {
			zone:          zone(1),
			expectedError: errorZoneCountOutOfBound(0),
		},
		"if spreadMinimizingZones contains more than maxZonesCount elements, errorZoneCountOutOfBound is returned": {
			zone:                  zone(1),
			spreadMinimizingZones: []string{zone(1), zone(2), zone(3), zone(4), zone(5), zone(6), zone(7), zone(8), zone(9)},
			expectedError:         errorZoneCountOutOfBound(9),
		},
		"if spreadMinimizingZones contains zone, succeed": {
			zone:                  zone(1),
			spreadMinimizingZones: []string{zone(1), zone(2)},
		},
		"if spreadMinimizingZones doesn't contain zone, errorZoneNotValid is returned": {
			zone:                  zone(3),
			spreadMinimizingZones: []string{zone(1), zone(2)},
			expectedError:         errorZoneNotValid(zone(3)),
		},
	}

	for _, testData := range tests {
		cfg := SpreadMinimizingConfig{testData.spreadMinimizingZones}
		instance := fmt.Sprintf("instance-%s-1", testData.zone)
		tokenGenerator, err := NewSpreadMinimizingTokenGenerator(cfg, instance, testData.zone, log.NewNopLogger())
		if testData.expectedError != nil {
			require.Error(t, err)
			require.Equal(t, testData.expectedError, err)
		} else {
			require.NoError(t, err)
			require.NotNil(t, tokenGenerator)
		}
	}
}

func TestSpreadMinimizingTokenGenerator_GenerateFirstInstanceTokens(t *testing.T) {
	cfg := &SpreadMinimizingConfig{zones}
	for z, zone := range zones {
		instance := fmt.Sprintf("instance-%s-%d", zone, 10)
		tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, instance, zone)
		tokens := tokenGenerator.generateFirstInstanceTokens()
		for i, token := range tokens {
			require.Equal(t, uint32(1<<23*i+z), token)
			require.True(t, token%uint32(maxZonesCount) == uint32(z))
		}
	}
}

func TestSpreadMinimizingTokenGenerator_GenerateFirstInstanceTokensIdempotent(t *testing.T) {
	cfg := &SpreadMinimizingConfig{zones}
	for _, zone := range zones {
		instance := fmt.Sprintf("instance-%s-%d", zone, 10)
		tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, instance, zone)
		tokens1 := tokenGenerator.generateFirstInstanceTokens()
		require.Len(t, tokens1, tokensPerInstance)
		tokens2 := tokenGenerator.generateFirstInstanceTokens()
		require.True(t, tokens1.Equals(tokens2))
	}
}

func TestSpreadMinimizingTokenGenerator_OptimalTokenOwnership(t *testing.T) {
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
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, nil, testInstance, testZone)
	for _, testData := range tests {
		optimalTokenOwnership := tokenGenerator.optimalTokenOwnership(testData.optimalInstanceOwnership, testData.currInstanceOwnership, testData.currTokensCount)
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
			ringToken:             ringToken{800, 40},
			optimalTokenOwnership: 80,
			expectedNewToken:      120,
		},
		"zoneID 1, prevToken < token": {
			ringToken:             ringToken{801, 41},
			optimalTokenOwnership: 80,
			expectedNewToken:      121,
		},
		"zoneID 2, prevToken < token": {
			ringToken:             ringToken{802, 42},
			optimalTokenOwnership: 80,
			expectedNewToken:      122,
		},
		"zoneID 0, prevToken > token": {
			ringToken:             ringToken{416, 4294967136},
			optimalTokenOwnership: 240,
			expectedNewToken:      88,
		},
		"zoneID 1, prevToken > token": {
			ringToken:             ringToken{417, 4294967137},
			optimalTokenOwnership: 240,
			expectedNewToken:      89,
		},
		"zoneID 2, prevToken > token": {
			ringToken:             ringToken{418, 4294967138},
			optimalTokenOwnership: 240,
			expectedNewToken:      90,
		},
		"zoneID 0, prevToken > token, offset > optimalTokenOwnership": {
			ringToken:             ringToken{416, 4294967136},
			optimalTokenOwnership: 120,
			expectedNewToken:      4294967256,
		},
		"zoneID 1, prevToken > token, offset > optimalTokenOwnership": {
			ringToken:             ringToken{417, 4294967137},
			optimalTokenOwnership: 120,
			expectedNewToken:      4294967257,
		},
		"zoneID 2, prevToken > token, offset > optimalTokenOwnership": {
			ringToken:             ringToken{418, 4294967138},
			optimalTokenOwnership: 120,
			expectedNewToken:      4294967258,
		},
		"bad congruence": {
			ringToken:             ringToken{90, 31},
			optimalTokenOwnership: 80,
			expectedError:         fmt.Errorf("calculation of a new token between 31 and 90 with optimal token ownership 80 was impossible: lower and upper bounds must be congruent modulo maximal allowed number of zones 8"),
		},
		"optimalTokenOwnership small": {
			ringToken:             ringToken{240, 80},
			optimalTokenOwnership: 2,
			expectedError:         fmt.Errorf("calculation of a new token between 80 and 240 with optimal token ownership 2 was impossible: optimal token ownership must be a positive multiple of maximal allowed number of zones 8"),
		},
		"optimalTokenOwnership bad congruence": {
			ringToken:             ringToken{240, 80},
			optimalTokenOwnership: 42,
			expectedError:         fmt.Errorf("calculation of a new token between 80 and 240 with optimal token ownership 42 was impossible: optimal token ownership must be a positive multiple of maximal allowed number of zones 8"),
		},
		"optimalTokenOwnership too big": {
			ringToken:             ringToken{240, 80},
			optimalTokenOwnership: 400,
			expectedError:         fmt.Errorf("calculation of a new token between 80 and 240 with optimal token ownership 400 was impossible: distance between lower and upper bound 160 is not big enough"),
		},
	}
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, nil, testInstance, testZone)
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
	cfg := &SpreadMinimizingConfig{zones}
	for instanceID := 0; instanceID < maxInstanceID; instanceID++ {
		for _, zone := range zones {
			instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
			tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, instance, zone)
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
	ownershipByInstanceByZone := registeredOwnershipByZone(instancesPerZone, instanceByToken, tokensByZone)
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
	cfg := &SpreadMinimizingConfig{zones}
	allTokens := make(map[uint32]bool, tokensPerInstance*(instanceID+1)*len(zones))
	for _, zone := range zones {
		instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
		tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, instance, zone)
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

func TestSpreadMinimizingTokenGenerator_GenerateAtMost512Tokens(t *testing.T) {
	cfg := &SpreadMinimizingConfig{zones}
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, testInstance, testZone)
	// we try to generate 2*optimalTokensPerInstance tokens, and we ensure
	// that only optimalTokensPerInstance tokens are generated
	tokens := tokenGenerator.GenerateTokens(2*optimalTokensPerInstance, nil)
	require.Len(t, tokens, optimalTokensPerInstance)
}

func TestSpreadMinimizingTokenGenerator_GenerateTokens(t *testing.T) {
	tokensPerInstance := 512
	instanceID := 1000
	zone := zones[0]
	instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
	cfg := &SpreadMinimizingConfig{zones}
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, instance, zone)
	// this is the set of all sorted tokens assigned to instance
	allTokens := tokenGenerator.generateAllTokens()
	require.Len(t, allTokens, tokensPerInstance)

	takenTokens := make(Tokens, 0, tokensPerInstance)
	tokensCount := 300
	// we get the first tokensCount tokens and ensure that they are returned in the same order
	// they have in allTokens
	tokens := tokenGenerator.GenerateTokens(tokensCount, takenTokens)
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

	// we ensure that further attempts to generate a positive number of tokens returns nothing
	noTokens := tokenGenerator.GenerateTokens(1, takenTokens)
	require.Len(t, noTokens, 0)
}

func BenchmarkSpreadMinimizingTokenGenerator_GenerateTokens(b *testing.B) {
	cfg := &SpreadMinimizingConfig{zones}
	tokenGenerator := createSpreadMinimizingTokenGenerator(b, cfg, testInstance, testZone)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokenGenerator.GenerateTokens(512, nil)
	}
}

func TestSpreadMinimizingTokenGenerator_GetMissingTokens(t *testing.T) {
	tokensPerInstance := 512
	cfg := &SpreadMinimizingConfig{zones}
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, testInstance, testZone)

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
	cfg := &SpreadMinimizingConfig{zones}
	for _, zone := range zones {
		instance := fmt.Sprintf("instance-%s-%d", zone, maxInstanceID)
		tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg, instance, zone)
		tokensByInstance := tokenGenerator.generateTokensByInstanceID()
		for id, tokens := range tokensByInstance {
			if !slices.IsSorted(tokens) {
				slices.Sort(tokens)
			}
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

func createSpreadMinimizingTokenGenerator(t testing.TB, cfg *SpreadMinimizingConfig, instance, zone string) *SpreadMinimizingTokenGenerator {
	if cfg == nil {
		cfg = &SpreadMinimizingConfig{zones}
	}
	tokenGenerator, err := NewSpreadMinimizingTokenGenerator(*cfg, instance, zone, log.NewLogfmtLogger(os.Stdout))
	require.NoError(t, err)
	require.NotNil(t, tokenGenerator)
	return tokenGenerator
}

// registeredOwnershipByZone calculates ownership maps grouped by instance id and by zone
func registeredOwnershipByZone(instancesPerZone int, instanceByToken map[uint32]*instanceInfo, tokensByZone map[string][]uint32) map[string]map[string]float64 {
	ownershipByInstanceByZone := make(map[string]map[string]float64, len(zones))
	for zone, tokens := range tokensByZone {
		ownershipByInstanceByZone[zone] = make(map[string]float64, instancesPerZone)
		if len(tokens) == 0 {
			continue
		}
		prev := len(tokens) - 1
		for tk, token := range tokens {
			ownership := float64(tokenDistance(tokens[prev], token))
			ownershipByInstanceByZone[zone][instanceByToken[token].InstanceID] += ownership
			prev = tk
		}
	}
	return ownershipByInstanceByZone
}
