package ring

import (
	"math"

	"github.com/pkg/errors"
	"golang.org/x/exp/slices" // using exp/slices until moving to go 1.21.
)

// TokenRanges describes token ranges owned by an instance.
// It consists of [start, end] pairs, where both start and end are inclusive.
type TokenRanges []uint32

func (tr TokenRanges) IncludesKey(key uint32) bool {
	switch {
	case len(tr) == 0:
		return false
	case key < tr[0]:
		// key comes before the first range
		return false
	case key > tr[len(tr)-1]:
		// key comes after the last range
		return false
	}

	index, found := slices.BinarySearch(tr, key)
	switch {
	case found:
		// ranges are closed
		return true
	case index%2 == 1:
		// hash would be inserted after the start of a range (even index)
		return true
	default:
		return false
	}
}

// GetTokenRangesForInstance returns the token ranges owned by an instance in the ring.
//
// Current implementation only works with multizone setup, where number of zones is equal to replication factor.
func (r *Ring) GetTokenRangesForInstance(instanceID string) (TokenRanges, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	instance, ok := r.ringDesc.Ingesters[instanceID]
	if !ok {
		return nil, ErrInstanceNotFound
	}
	if instance.Zone == "" {
		return nil, errors.New("zone not set")
	}

	rf := r.cfg.ReplicationFactor
	numZones := len(r.ringTokensByZone)

	// To simplify computation of token ranges, we currently only support case where zone-awareness is enabled,
	// and replicaction factor is equal to number of zones.
	if !r.cfg.ZoneAwarenessEnabled || rf != numZones {
		// if zoneAwareness is disabled we need to treat the whole ring as one big zone, and we would
		// need to walk the ring backwards looking for RF-1 tokens from other instances to determine the range.
		return nil, errors.New("can't use ring configuration for computing token ranges")
	}

	// at this point zone-aware replication is enabled, and rf == numZones
	// this means that we will write to one replica in each zone, so we can just consider the zonal ring for our instance
	subringTokens, ok := r.ringTokensByZone[instance.Zone]
	if !ok || len(subringTokens) == 0 {
		return nil, errors.New("no tokens for zone")
	}

	ranges := make([]uint32, 0, 2*(len(instance.Tokens)+1)) // 1 range (2 values) per token + one additional if we need to split the rollover range

	tokenInfo, ok := r.ringInstanceByToken[subringTokens[0]]
	if !ok {
		// This should never happen unless there's a bug in the ring code.
		return nil, ErrInconsistentTokensInfo
	}
	firstTokenOwned := tokenInfo.InstanceID == instanceID

	addMaxSingleton := false
	currIndex := 0
	rangeStart := uint32(0)
	rangeEnd := uint32(math.MaxUint32)
	for {
		var token uint32
		// We are looking for the highest token not owned by instanceID,
		// to add it as the next sub-range start.
		for currIndex < len(subringTokens) {
			token = subringTokens[currIndex]
			tokenInfo, ok := r.ringInstanceByToken[token]
			if !ok {
				// This should never happen unless there's a bug in the ring code.
				return nil, ErrInconsistentTokensInfo
			}
			if tokenInfo.InstanceID == instanceID {
				break
			}
			rangeStart = token
			currIndex++
		}

		if currIndex == len(subringTokens) {
			if firstTokenOwned {
				// If we reach the end of the ring without finding other tokens owned by instanceID,
				// another sub-range is added if the first token is owned by instanceID.
				ranges = append(ranges, rangeStart, math.MaxUint32)
			}
			break
		}

		// At this point we have the next sub-range start, and we are looking for the next sub-range end.
		if token != 0 {
			rangeEnd = token - 1
		}
		currIndex++
		// We are looking for the highest token owned by instanceID,
		// to add it as the next sub-range end.
		for currIndex < len(subringTokens) {
			token = subringTokens[currIndex]
			tokenInfo, ok := r.ringInstanceByToken[token]
			if !ok {
				// This should never happen unless there's a bug in the ring code.
				return nil, ErrInconsistentTokensInfo
			}
			if tokenInfo.InstanceID != instanceID {
				break
			}
			rangeEnd = token - 1
			currIndex++
		}

		// If we reached the end of the ring having only the tokens owned by instanceID,
		// we add the last sub-range and terminate.
		if currIndex == len(subringTokens) {
			if firstTokenOwned {
				rangeEnd = math.MaxUint32
			}
			ranges = append(ranges, rangeStart, rangeEnd)
			break
		}

		// At this point we have the next sub-range end.
		if rangeEnd == math.MaxUint32 {
			addMaxSingleton = true
		} else {
			ranges = append(ranges, rangeStart, rangeEnd)
		}
		rangeStart = token
		currIndex++
	}

	if addMaxSingleton && ranges[len(ranges)-1] != math.MaxUint32 {
		ranges = append(ranges, math.MaxUint32, math.MaxUint32)
	}

	return ranges, nil
}
