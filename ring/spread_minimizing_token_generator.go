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

const (
	totalTokensCount         = 1 << 32
	optimalTokensPerInstance = 1 << 9
	maxZonesCount            = 1 << 3
)

var (
	instanceIDRegex          = regexp.MustCompile(`^(.*)-(zone-[a-z])-(\d+)$`)
	errorBadInstanceIDFormat = func(instanceID string) error {
		return fmt.Errorf("unable to extract instance id from \"%s\"", instanceID)
	}
	errorZoneNotValid = func(zone string) error {
		return fmt.Errorf("zone %s is not valid", zone)
	}
	errorMultipleOfZonesCount = func(optimalTokenOwnership uint32, token ringToken) error {
		return fmt.Errorf("calculation of a new token between %d and %d with optimal token ownership %d was impossible: optimal token ownership must be a positive multiple of maximal allowed number of zones %d", token.prevToken, token.token, optimalTokenOwnership, maxZonesCount)
	}
	errorLowerAndUpperBoundModulo = func(optimalTokenOwnership uint32, token ringToken) error {
		return fmt.Errorf("calculation of a new token between %d and %d with optimal token ownership %d was impossible: lower and upper bounds must be congruent modulo maximal allowed number of zones %d", token.prevToken, token.token, optimalTokenOwnership, maxZonesCount)
	}
	errorDistanceBetweenTokensNotBigEnough = func(optimalTokenOwnership, ownership int, token ringToken) error {
		return fmt.Errorf("calculation of a new token between %d and %d with optimal token ownership %d was impossible: distance between lower and upper bound %d is not big enough", token.prevToken, token.token, optimalTokenOwnership, ownership)
	}
)

type SpreadMinimizingConfig struct {
	InstanceID string
	Zone       string
}

type SpreadMinimizingTokenGenerator struct {
	cfg        SpreadMinimizingConfig
	instanceID int
	zoneID     int
	zones      []string
	logger     log.Logger
}

func NewSpreadMinimizingTokenGenerator(cfg SpreadMinimizingConfig, zones []string, logger log.Logger) (*SpreadMinimizingTokenGenerator, error) {
	sortedZones := make([]string, len(zones))
	copy(sortedZones, zones)
	if !slices.IsSorted(sortedZones) {
		sort.Strings(sortedZones)
	}
	instanceID, err := parseInstanceID(cfg.InstanceID)
	if err != nil {
		return nil, err
	}
	zoneID, err := findZoneID(cfg.Zone, sortedZones)
	if err != nil {
		return nil, err
	}

	tokenGenerator := &SpreadMinimizingTokenGenerator{
		cfg:        cfg,
		instanceID: instanceID,
		zoneID:     zoneID,
		zones:      sortedZones,
		logger:     logger,
	}
	return tokenGenerator, nil
}

func parseInstanceID(instanceID string) (int, error) {
	parts := instanceIDRegex.FindStringSubmatch(instanceID)
	if len(parts) != 4 {
		return 0, errorBadInstanceIDFormat(instanceID)
	}
	return strconv.Atoi(parts[3])
}

// findZoneID gets a zone name and a slice of sorted zones,
// and return the index of the zone in the slice.
func findZoneID(zone string, sortedZones []string) (int, error) {
	if !slices.IsSorted(sortedZones) {
		slices.Sort(sortedZones)
	}
	index := sort.SearchStrings(sortedZones, zone)
	if index >= len(sortedZones) {
		return 0, errorZoneNotValid(zone)
	}
	return index, nil
}

// generateFirstInstanceTokens calculates a set of tokens that should be assigned to the first instance (with id 0)
// of the zone of the underlying instance.
func (t *SpreadMinimizingTokenGenerator) generateFirstInstanceTokens() Tokens {
	tokenDistance := (totalTokensCount / optimalTokensPerInstance / maxZonesCount) * maxZonesCount
	tokens := make(Tokens, 0, optimalTokensPerInstance)
	for i := 0; i < optimalTokensPerInstance; i++ {
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
	if optimalTokenOwnership < maxZonesCount || optimalTokenOwnership%maxZonesCount != 0 {
		return 0, errorMultipleOfZonesCount(optimalTokenOwnership, token)
	}
	if token.prevToken%maxZonesCount != token.token%maxZonesCount {
		return 0, errorLowerAndUpperBoundModulo(optimalTokenOwnership, token)
	}
	ownership := tokenDistance(token.prevToken, token.token)
	if ownership <= int(optimalTokenOwnership) {
		return 0, errorDistanceBetweenTokensNotBigEnough(int(optimalTokenOwnership), ownership, token)
	}
	maxTokenValue := (math.MaxUint32/uint32(maxZonesCount) - 1) * maxZonesCount
	offset := maxTokenValue - token.prevToken
	if offset < optimalTokenOwnership {
		newToken := optimalTokenOwnership - offset
		return newToken, nil
	}
	return token.prevToken + optimalTokenOwnership, nil
}

// optimalTokenOwnership calculates the optimal ownership of the remaining currTokensCount tokens of an instance
// having the given current instances ownership currInstanceOwnership and the given optimal instance ownership
// optimalInstanceOwnership. The resulting token ownership must be a multiple of the number of zones.
func (t *SpreadMinimizingTokenGenerator) optimalTokenOwnership(optimalInstanceOwnership, currInstanceOwnership float64, currTokensCount uint32) uint32 {
	optimalTokenOwnership := uint32(optimalInstanceOwnership-currInstanceOwnership) / currTokensCount
	return (optimalTokenOwnership / maxZonesCount) * maxZonesCount
}

// GenerateTokens returns at most requestedTokensCount unique tokens, none of which clashes with the given
// allTakenTokens, representing the set of all tokens currently present in the ring. Returned tokens are sorted.
// The optimal number of tokens (optimalTokenPerInstance), i.e., 512, reserved for the underlying instance are
// generated by generateAllTokens. GenerateTokens selects the first requestedTokensCount tokens from the reserved
// tokens set, that are not already present in the takenTokens.
// The number of returned tokens might be lower than the requested number of tokens in the following cases:
//   - if tokensCount is higher than 512 (optimalTokensPerInstance), or
//   - if among the 512 (optimalTokenPerInstance) reserved tokens there is less than tokenCount
//     tokens not already present in takenTokens.
func (t *SpreadMinimizingTokenGenerator) GenerateTokens(requestedTokensCount int, allTakenTokens []uint32) Tokens {
	used := make(map[uint32]bool, len(allTakenTokens))
	for _, v := range allTakenTokens {
		used[v] = true
	}

	allTokens := t.generateAllTokens()
	tokens := make(Tokens, 0, requestedTokensCount)

	// allTokens is a sorted slice of tokens for instance t.cfg.InstanceID in zone t.cfg.zone
	// We filter out tokens from allTakenTokens, if any, and return at most requestedTokensCount tokens.
	for i := 0; i < len(allTokens) && len(tokens) < requestedTokensCount; i++ {
		token := allTokens[i]
		if used[token] {
			continue
		}
		tokens = append(tokens, token)
	}
	return tokens
}

// generateAllTokens generates the optimal number of tokens (optimalTokenPerInstance), i.e., 512,
// for the underlying instance (with id t.instanceID). Generated tokens are sorted, and they are
// distributed in such a way that registered ownership of the instance t.instanceID, when it is
// placed in the ring that already contains instances with all the ids lower that t.instanceID
// is optimal.
// Calls to this method will always return the same set of tokens.
func (t *SpreadMinimizingTokenGenerator) generateAllTokens() Tokens {
	firstInstanceTokens := t.generateFirstInstanceTokens()

	if t.instanceID == 0 {
		return firstInstanceTokens
	}

	// tokensQueues is a slice of priority queues. Slice indexes correspond
	// to the ids of instances, while priority queues represent the tokens
	// of the corresponding instance, ordered from highest to lowest ownership.
	tokensQueues := make([]ownershipPriorityQueue[ringToken], t.instanceID)

	// Create and initialize priority queue of tokens for the first instance
	tokensQueue := newPriorityQueue[ringToken](optimalTokensPerInstance)
	prev := len(firstInstanceTokens) - 1
	firstInstanceOwnership := 0.0
	for tk, token := range firstInstanceTokens {
		tokenOwnership := float64(tokenDistance(firstInstanceTokens[prev], token))
		firstInstanceOwnership += tokenOwnership
		heap.Push(&tokensQueue, newRingTokenOwnershipInfo(token, firstInstanceTokens[prev]))
		prev = tk
	}
	tokensQueues[0] = tokensQueue

	// instanceQueue is a priority queue of instances such that instances with higher ownership have a higher priority
	instanceQueue := newPriorityQueue[ringInstance](t.instanceID)
	heap.Push(&instanceQueue, newRingInstanceOwnershipInfo(0, firstInstanceOwnership))

	// ignoredInstances is a slice of the current instances whose tokens
	// don't have enough space to accommodate new tokens.
	ignoredInstances := make([]ownershipInfo[ringInstance], 0, t.instanceID)

	for i := 1; i <= t.instanceID; i++ {
		optimalInstanceOwnership := float64(totalTokensCount) / float64(i+1)
		currInstanceOwnership := 0.0
		addedTokens := 0
		ignoredInstances = ignoredInstances[:0]
		tokens := make(Tokens, 0, optimalTokensPerInstance)
		// currInstanceTokenQueue is the priority queue of tokens of newInstance
		currInstanceTokenQueue := newPriorityQueue[ringToken](optimalTokensPerInstance)
		for addedTokens < optimalTokensPerInstance {
			optimalTokenOwnership := t.optimalTokenOwnership(optimalInstanceOwnership, currInstanceOwnership, uint32(optimalTokensPerInstance-addedTokens))
			highestOwnershipInstance := instanceQueue.Peek()
			if highestOwnershipInstance == nil || highestOwnershipInstance.ownership <= float64(optimalTokenOwnership) {
				level.Warn(t.logger).Log("msg", "it was impossible to add a token because the instance with the highest ownership cannot satisfy the request", "added tokens", addedTokens+1, "highest ownership", highestOwnershipInstance.ownership, "requested ownership", optimalTokenOwnership)
				// if this happens, it means that we cannot accommodate other tokens, so we panic
				err := fmt.Errorf("it was impossible to add %dth token for instance with id %d in zone %s because the instance with the highest ownership cannot satisfy the requested ownership %d", addedTokens+1, i, t.cfg.Zone, optimalTokenOwnership)
				panic(err)
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
				// if this happens, it means that we cannot accommodate additional tokens, so we panic
				err := fmt.Errorf("it was impossible to calculate the %dth token for instance with id %d in zone %s", addedTokens+1, i, t.cfg.Zone)
				panic(err)
			}
			tokens = append(tokens, newToken)
			// add the new token to currInstanceTokenQueue
			heap.Push(&currInstanceTokenQueue, newRingTokenOwnershipInfo(newToken, token.prevToken))

			oldTokenOwnership := highestOwnershipToken.ownership
			newTokenOwnership := float64(tokenDistance(newToken, token.token))
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

		tokensQueues[i] = currInstanceTokenQueue

		// add the current instance with the calculated ownership currInstanceOwnership to instanceQueue
		heap.Push(&instanceQueue, newRingInstanceOwnershipInfo(i, currInstanceOwnership))
	}

	return nil
}
