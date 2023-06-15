package ring

import (
	"container/heap"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"golang.org/x/exp/slices"
)

const totalTokensCount = 1 << 32

var (
	instanceIDRegex          = regexp.MustCompile(`^(.*)-(zone-[a-z])-(\d+)$`)
	errorBadInstanceIDFormat = func(instanceID string) error {
		return fmt.Errorf("unable to extract instance id from \"%s\"", instanceID)
	}
	errorZoneNotValid = func(zone string) error {
		return fmt.Errorf("zone %s is not valid", zone)
	}
	errorMultipleOfZonesCount = func(zonesCount, optimalTokenOwnership uint32, token ringToken) error {
		return fmt.Errorf("calculation of a new token between %d and %d with optimal token ownership %d was impossible: optimal token ownership must be a positive multiple of number of zones %d", token.prevToken, token.token, optimalTokenOwnership, zonesCount)
	}
	errorLowerAndUpperBoundModulo = func(zonesCount, optimalTokenOwnership uint32, token ringToken) error {
		return fmt.Errorf("calculation of a new token between %d and %d with optimal token ownership %d was impossible: lower and upper bounds must be congruent modulo number of zones %d", token.prevToken, token.token, optimalTokenOwnership, zonesCount)
	}
	errorDistanceBetweenTokensNotBigEnough = func(optimalTokenOwnership, ownership int, token ringToken) error {
		return fmt.Errorf("calculation of a new token between %d and %d with optimal token ownership %d was impossible: distance between lower and upper bound %d is not big enough", token.prevToken, token.token, optimalTokenOwnership, ownership)
	}
)

type SpreadMinimizingConfig struct {
	instanceID        string
	zone              string
	tokensPerInstance int
}

func NewSpreadMinimizingConfig(instanceID, zone string, tokensPerInstance int) *SpreadMinimizingConfig {
	return &SpreadMinimizingConfig{
		instanceID:        instanceID,
		zone:              zone,
		tokensPerInstance: tokensPerInstance,
	}
}

type SpreadMinimizingTokenGenerator struct {
	cfg        *SpreadMinimizingConfig
	instanceID int
	zoneID     int
	zones      []string
	logger     log.Logger
}

func NewSpreadMinimizingTokenGenerator(cfg *SpreadMinimizingConfig, zones []string, logger log.Logger) (*SpreadMinimizingTokenGenerator, error) {
	if !slices.IsSorted(zones) {
		sort.Strings(zones)
	}
	instanceID, err := getInstanceID(cfg.instanceID)
	if err != nil {
		return nil, err
	}
	zoneID, err := getZoneID(cfg.zone, zones)
	if err != nil {
		return nil, err
	}

	tokenGenerator := &SpreadMinimizingTokenGenerator{
		cfg:        cfg,
		instanceID: instanceID,
		zoneID:     zoneID,
		zones:      zones,
		logger:     logger,
	}
	return tokenGenerator, nil
}

func getInstanceID(instanceID string) (int, error) {
	parts := instanceIDRegex.FindStringSubmatch(instanceID)
	if len(parts) != 4 {
		return -1, errorBadInstanceIDFormat(instanceID)
	}
	id, err := strconv.Atoi(parts[3])
	if err != nil {
		return -1, err
	}
	return id, nil
}

func getZoneID(zone string, zones []string) (int, error) {
	if !slices.IsSorted(zones) {
		slices.Sort(zones)
	}
	index := sort.SearchStrings(zones, zone)
	if index >= len(zones) {
		return -1, errorZoneNotValid(zone)
	}
	return index, nil
}

// generateFirstInstanceTokens calculates a set of tokens that should be assigned to the first instance (with id 0)
// of the zone of the underlying instance.
func (t *SpreadMinimizingTokenGenerator) generateFirstInstanceTokens() Tokens {
	zonesCount := len(t.zones)
	tokensPerInstance := t.cfg.tokensPerInstance
	tokenDistance := (totalTokensCount / tokensPerInstance / zonesCount) * zonesCount
	tokens := make(Tokens, 0, tokensPerInstance)
	for i := 0; i < tokensPerInstance; i++ {
		token := uint32(i*tokenDistance) + uint32(t.zoneID)
		tokens = append(tokens, token)
	}
	return tokens
}

// calculateNewToken determines where in the range represented by the given ringToken should a new token be placed
// in order to satisfy the constraint represented by the optimalTokenOwnership. This method assumes that:
// - ringToken.token % zonesCount == ringToken.prevToken % zonesCount
// - optimalTokenOwnership % zonesCount == 0,
// where zonesCount is the number of zones in the ring. The caller of this function must ensure that these assumptions hold.
func (t *SpreadMinimizingTokenGenerator) calculateNewToken(token ringToken, optimalTokenOwnership uint32) (uint32, error) {
	zonesCount := uint32(len(t.zones))
	if optimalTokenOwnership < zonesCount || optimalTokenOwnership%zonesCount != 0 {
		return 0, errorMultipleOfZonesCount(zonesCount, optimalTokenOwnership, token)
	}
	if token.prevToken%zonesCount != token.token%zonesCount {
		return 0, errorLowerAndUpperBoundModulo(zonesCount, optimalTokenOwnership, token)
	}
	ownership := getTokenDistance(token.prevToken, token.token)
	if ownership <= int(optimalTokenOwnership) {
		return 0, errorDistanceBetweenTokensNotBigEnough(int(optimalTokenOwnership), ownership, token)
	}
	maxTokenValue := (math.MaxUint32/zonesCount - 1) * zonesCount
	offset := maxTokenValue - token.prevToken
	if offset < optimalTokenOwnership {
		newToken := optimalTokenOwnership - offset
		return newToken, nil
	}
	return token.prevToken + optimalTokenOwnership, nil
}

// getOptimalTokenOwnership calculates the optimal ownership of the remaining currTokensCount tokens of an instance
// having the given current instances ownership currInstanceOwnership and the given optimal instance ownership
// optimalInstanceOwnership. The resulting token ownership must be a multiple of the number of zones.
func (t *SpreadMinimizingTokenGenerator) getOptimalTokenOwnership(optimalInstanceOwnership, currInstanceOwnership float64, currTokensCount uint32) uint32 {
	optimalTokenOwnership := uint32(optimalInstanceOwnership-currInstanceOwnership) / currTokensCount
	zonesCount := uint32(len(t.zones))
	optimalTokenOwnership = (optimalTokenOwnership / zonesCount) * zonesCount
	return optimalTokenOwnership
}

// GenerateTokens generates at most tokensCount unique tokens, none of which clash with the given takenTokens.
// Generated tokens are sorted. The number of returned tokens might be less than requested in the following cases:
//   - if tokensCount is higher than the maximal configured number of tokens of the
//     underlying instance (t.cfg.tokensPerInstance).
//   - if among t.cfg.tokensPerInstance tokens reserved for the underlying instance
//     there is less than tokenCount tokens not already present in takenTokens.
func (t *SpreadMinimizingTokenGenerator) GenerateTokens(tokensCount int, takenTokens []uint32) Tokens {
	used := make(map[uint32]bool, len(takenTokens))
	for _, v := range takenTokens {
		used[v] = true
	}

	allTokens := t.generateAllTokens()
	tokens := make(Tokens, 0, tokensCount)

	// allTokens is a sorted slice of tokens for instance t.cfg.instanceID in zone t.cfg.zone
	// We filter out tokens from takenTokens, if any, and return at most tokensCount tokens.
	for i := 0; i < len(allTokens) && len(tokens) < tokensCount; i++ {
		token := allTokens[i]
		if used[token] {
			continue
		}
		tokens = append(tokens, token)
	}
	return tokens
}

// generateAllTokens generates the configured number of tokens (t.cfg.tokensPerInstance) for the underlying instance.
// Generated tokens are sorted. Calls to this method will always return the same set of tokens.
func (t *SpreadMinimizingTokenGenerator) generateAllTokens() Tokens {
	firstInstanceTokens := t.generateFirstInstanceTokens()

	if t.instanceID == 0 {
		return firstInstanceTokens
	}

	tokensCount := t.cfg.tokensPerInstance
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

	for i := 1; i <= t.instanceID; i++ {
		optimalInstanceOwnership := float64(totalTokensCount) / float64(i+1)
		currInstanceOwnership := 0.0
		addedTokens := 0
		// ignoredInstances is a slice of the current instances whose tokens
		// don't have enough space to accommodate new tokens.
		ignoredInstances := make([]ownershipInfo[ringInstance], 0, i)
		tokens := make(Tokens, 0, tokensCount)
		// currInstanceTokenQueue is the priority queue of tokens of newInstance
		currInstanceTokenQueue := newPriorityQueue[ringToken](tokensCount)
		for addedTokens < tokensCount {
			optimalTokenOwnership := t.getOptimalTokenOwnership(optimalInstanceOwnership, currInstanceOwnership, uint32(tokensCount-addedTokens))
			highestOwnershipInstance := instanceQueue.Peek()
			if highestOwnershipInstance == nil || highestOwnershipInstance.ownership <= float64(optimalTokenOwnership) {
				level.Warn(t.logger).Log("msg", "it was impossible to add a token because the instance with the highest ownership cannot satisfy the request", "added tokens", addedTokens+1, "highest ownership", highestOwnershipInstance.ownership, "requested ownership", optimalTokenOwnership)
				// if this happens, we return the current set of tokens, although it is less than t.cfg.tokensPerInstance
				break
			}
			tokensQueue := tokensQueues[highestOwnershipInstance.item.instanceID]
			highestOwnershipToken := tokensQueue.Peek()
			if highestOwnershipToken.ownership <= float64(optimalTokenOwnership) {
				// The token with the highest ownership of the instance with the highest ownership could not
				// accommodate a new token, hence we ignore this instance and pass to the next instance.
				ignoredInstances = append(ignoredInstances, heap.Pop(&instanceQueue).(ownershipInfo[ringInstance]))
				continue
			}
			token := highestOwnershipToken.item
			newToken, err := t.calculateNewToken(token, optimalTokenOwnership)
			if err != nil {
				level.Error(t.logger).Log("msg", "it was impossible to calculate a new token because an error occurred", "err", err)
				// if this happens, we return the current set of tokens, although it is less than t.cfg.tokensPerInstance
				break
			}
			tokens = append(tokens, newToken)
			// add the new token to currInstanceTokenQueue
			currInstanceTokenQueue.Add(newRingTokenOwnershipInfo(newToken, token.prevToken))

			oldTokenOwnership := highestOwnershipToken.ownership
			newTokenOwnership := float64(getTokenDistance(newToken, token.token))
			currInstanceOwnership += oldTokenOwnership - newTokenOwnership

			// The token with the highest ownership of the instance with the highest ownership has changed,
			// so we propagate these changes in the corresponding tokens queue.
			highestOwnershipToken.item.prevToken = newToken
			highestOwnershipToken.ownership = newTokenOwnership
			heap.Fix(&tokensQueue, 0)

			// The ownership of the instance with the highest ownership has changed,
			// so we propagate these changes in the instances queue.
			highestOwnershipInstance.ownership = highestOwnershipInstance.ownership - oldTokenOwnership + newTokenOwnership
			heap.Fix(&instanceQueue, 0)

			addedTokens++
		}
		if i == t.instanceID {
			slices.Sort(tokens)
			return tokens
		}

		// If there were some ignored instances, we put them back on the queue.
		for _, ignoredInstance := range ignoredInstances {
			heap.Push(&instanceQueue, ignoredInstance)
		}

		heap.Init(&currInstanceTokenQueue)
		tokensQueues[i] = currInstanceTokenQueue

		// add the current instance with the calculated ownership currInstanceOwnership to instanceQueue
		heap.Push(&instanceQueue, newRingInstanceOwnershipInfo(i, currInstanceOwnership))
	}

	return nil
}
