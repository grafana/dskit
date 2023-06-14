package ring

import (
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

var (
	zones = []string{"zone-a", "zone-b", "zone-c"}
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
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, nil)
	zonesCount := len(zones)
	tokensPerInstance := 512
	for z := range zones {
		tokens, err := tokenGenerator.generateFirstInstanceTokens(z, tokensPerInstance)
		require.NoError(t, err)
		for i, token := range tokens {
			require.Equal(t, uint32(i<<23/zonesCount*zonesCount+z), token)
			require.True(t, token%uint32(zonesCount) == uint32(z))
		}
	}
}

func TestSpreadMinimizingTokenGenerator_ZoneSetTooBig(t *testing.T) {
	tokensPerInstance := 1 << 31
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, nil)
	for z := range zones {
		_, err := tokenGenerator.generateFirstInstanceTokens(z, tokensPerInstance)
		require.Error(t, err)
		require.Equal(t, errorZoneSetTooBig(len(zones), tokensPerInstance), err)
	}
}

func TestSpreadMinimizingTokenGenerator_GenerateFirstInstanceTokensIdempotent(t *testing.T) {
	tokensPerInstance := 512
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, nil)
	for z := range tokenGenerator.zones {
		tokens1, err := tokenGenerator.generateFirstInstanceTokens(z, tokensPerInstance)
		require.NoError(t, err)
		tokens2, err := tokenGenerator.generateFirstInstanceTokens(z, tokensPerInstance)
		require.NoError(t, err)
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
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, nil)
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
	tokenGenerator := createSpreadMinimizingTokenGenerator(t, nil)
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

func TestSpreadMinimizingTokenGenerator_GenerateTokensIdempotent(t *testing.T) {
	tokensPerInstance := 512
	maxInstanceID := 128
	for instanceID := 0; instanceID < maxInstanceID; instanceID++ {
		for _, zone := range zones {
			instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
			cfg := NewSpreadMinimizingConfig(instance, zone, false)
			tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg)
			tokens1, err := tokenGenerator.GenerateTokens(tokensPerInstance, nil)
			require.NoError(t, err)
			require.Len(t, tokens1, tokensPerInstance)
			tokens2, err := tokenGenerator.GenerateTokens(tokensPerInstance, nil)
			require.NoError(t, err)
			require.True(t, tokens1.Equals(tokens2))
		}
	}
}

func TestSpreadMinimizingRing_CalculateTokensByInstanceUnique(t *testing.T) {
	tokensPerInstance := 512
	maxInstancesPerZone := 128
	allTokens := make(Tokens, 0, maxInstancesPerZone*tokensPerInstance*len(zones))
	for instanceID := 0; instanceID < maxInstancesPerZone; instanceID++ {
		for _, zone := range zones {
			instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
			cfg := NewSpreadMinimizingConfig(instance, zone, true)
			tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg)
			tokens, err := tokenGenerator.GenerateTokens(tokensPerInstance, allTokens)
			require.NoError(t, err)
			allTokens = append(allTokens, tokens...)
		}
	}
}

func TestSpreadMinimizingRing_VerifyTokensByZone(t *testing.T) {
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

func TestSpreadMinimizingRing_VerifyInstanceOwnershipSpreadByZone(t *testing.T) {
	tokensPerInstance := 512
	instancesPerZone := 128
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

func createTokensForAllInstancesAndZones(t *testing.T, instancesPerZone, tokensPerInstance int) (map[uint32]*instanceInfo, map[string][]uint32) {
	instanceByToken := make(map[uint32]*instanceInfo, instancesPerZone*tokensPerInstance*len(zones))
	tokenSetsByZone := make(map[string][][]uint32, len(zones))
	for instanceID := 0; instanceID < instancesPerZone; instanceID++ {
		for _, zone := range zones {
			instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
			cfg := NewSpreadMinimizingConfig(instance, zone, false)
			tokenGenerator := createSpreadMinimizingTokenGenerator(t, cfg)
			tokens, err := tokenGenerator.GenerateTokens(tokensPerInstance, nil)
			require.NoError(t, err)
			instInfo := &instanceInfo{
				InstanceID: instance,
				Zone:       zone,
			}
			for _, token := range tokens {
				instanceByToken[token] = instInfo
			}
			allTokens, ok := tokenSetsByZone[zone]
			if !ok {
				allTokens = make([][]uint32, 0, instancesPerZone)
			}
			allTokens = append(allTokens, tokens)
			tokenSetsByZone[zone] = allTokens
		}
	}
	tokensByZone := MergeTokensByZone(tokenSetsByZone)
	return instanceByToken, tokensByZone
}

func createSpreadMinimizingTokenGenerator(t *testing.T, cfg *SpreadMinimizingConfig) *SpreadMinimizingTokenGenerator {
	if cfg == nil {
		cfg = NewSpreadMinimizingConfig("instance-zone-a-10", "zone-a", false)
	}
	tokenGenerator, err := NewSpreadMinimizingTokenGenerator(cfg, zones, log.NewLogfmtLogger(os.Stdout))
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
