package ring

import (
	"container/heap"
	"container/list"
	"fmt"
	"math"
	"strings"
	"time"
)

type partitionRingTokenShuffler interface {
	// shuffle gets a PartitionRing, and a map of timeseries ownership by token,
	// and builds a new PartitionRing with the same partitions and tokens as the original
	// ring, but possibly with a different distribution of tokens among partitions.
	shuffle(PartitionRing, map[Token]float64, bool) *PartitionRing
}

type preserveConsistencyPartitionRingTokenShuffler struct {
	partitionRingTokenShuffler
	duration time.Duration
}

func (s *preserveConsistencyPartitionRingTokenShuffler) minMaxOwnership(partitionsByToken map[Token]*list.List, timeseriesOwnershipByToken map[Token]float64) (int32, float64, int32, float64, bool) {
	timeseriesOwnershipByPartition := make(map[int32]float64)
	for token, partitions := range partitionsByToken {
		// We consider only partitions with tokens that haven't been already reshuffled.
		if partitions.Len() == 1 {
			activePartition := partitions.Front().Value.(*partition)
			timeseriesOwnership := timeseriesOwnershipByToken[token]
			timeseriesOwnershipByPartition[activePartition.id] = timeseriesOwnershipByPartition[activePartition.id] + timeseriesOwnership
		}
	}

	if len(timeseriesOwnershipByPartition) == 0 {
		return 0, 0, 0, 0, false
	}

	minTimeseriesOwnership := math.MaxFloat64
	maxTimeseriesOwnership := 0.0
	minPartition := int32(-1)
	maxPartition := int32(-1)

	for partition, timeseriesOwnership := range timeseriesOwnershipByPartition {
		if timeseriesOwnership < minTimeseriesOwnership {
			minTimeseriesOwnership = timeseriesOwnership
			minPartition = partition
		}
		if timeseriesOwnership > maxTimeseriesOwnership {
			maxTimeseriesOwnership = timeseriesOwnership
			maxPartition = partition
		}
	}

	return minPartition, minTimeseriesOwnership, maxPartition, maxTimeseriesOwnership, true
}

func (s *preserveConsistencyPartitionRingTokenShuffler) createPartitionTokensPQs(ring PartitionRing, timeseriesOwnershipByToken map[Token]float64, minPartition int32, maxPartition int32) (ownershipPriorityQueue[ringInstance], ownershipPriorityQueue[ringInstance]) {
	maxPartitionTokensPQ := newPriorityQueue[ringInstance](optimalTokensPerInstance, true)
	minPartitionTokensPQ := newPriorityQueue[ringInstance](optimalTokensPerInstance, false)

	for token := range ring.partitionsByToken {
		activePartitionID, ok := ring.activePartitionForToken(token)
		if !ok {
			continue
		}
		timeseriesOwnership := timeseriesOwnershipByToken[token]
		if activePartitionID == minPartition {
			item := newRingInstanceOwnershipInfo(int(token), timeseriesOwnership)
			heap.Push(&minPartitionTokensPQ, item)
		} else if activePartitionID == maxPartition {
			item := newRingInstanceOwnershipInfo(int(token), timeseriesOwnership)
			heap.Push(&maxPartitionTokensPQ, item)
		}
	}
	return minPartitionTokensPQ, maxPartitionTokensPQ
}

func (s *preserveConsistencyPartitionRingTokenShuffler) shuffle(ring PartitionRing, timeseriesOwnershipByToken map[Token]float64, printSteps bool) *PartitionRing {
	ring.cleanupExpiredPartitions(s.duration)
	minPartition, minTimeseriesOwnership, maxPartition, maxTimeseriesOwnership, found := s.minMaxOwnership(ring.partitionsByToken, timeseriesOwnershipByToken)

	if !found {
		fmt.Println("It wasn't possible to determine min and max info")
		return &ring
	}

	if printSteps {
		fmt.Printf("Partitions with min and max timeseries ownership are %d (%10.3f) and %d (%10.3f)\n", minPartition, minTimeseriesOwnership, maxPartition, maxTimeseriesOwnership)
	}

	minPartitionTokensPQ, maxPartitionTokensPQ := s.createPartitionTokensPQs(ring, timeseriesOwnershipByToken, minPartition, maxPartition)

	for minTimeseriesOwnership < maxTimeseriesOwnership {
		maxToken := heap.Pop(&maxPartitionTokensPQ).(ownershipInfo[ringInstance])
		minToken := heap.Pop(&minPartitionTokensPQ).(ownershipInfo[ringInstance])
		if printSteps {
			fmt.Printf("\ttoken %d (%10.3f) has been moved from partition %d to partition %d\n", maxToken.item.key(), maxToken.ownership, maxPartition, minPartition)
			fmt.Printf("\ttoken %d (%10.3f) has been moved from partition %d to partition %d\n", minToken.item.key(), minToken.ownership, minPartition, maxPartition)
		}
		heap.Push(&maxPartitionTokensPQ, minToken)
		heap.Push(&minPartitionTokensPQ, maxToken)
		maxTimeseriesOwnership = maxTimeseriesOwnership - maxToken.ownership + minToken.ownership
		minTimeseriesOwnership = minTimeseriesOwnership - minToken.ownership + maxToken.ownership
		if printSteps {
			fmt.Printf("\tnew timeseries ownerships are %d (%10.3f) and %d (%10.3f)\n", minPartition, minTimeseriesOwnership, maxPartition, maxTimeseriesOwnership)
			fmt.Println(strings.Repeat("-", 50))
		}
	}

	desc := ring.desc
	if len(ring.partitionsByToken) == 0 {
		ring.partitionsByToken = make(map[Token]*list.List, 2)
	}

	for partition, partitionDesc := range desc.Partitions {
		if partition == minPartition {
			s.updateTokensFromPQ(&partitionDesc, minPartitionTokensPQ, ring.partitionsByToken)
		} else if partition == maxPartition {
			s.updateTokensFromPQ(&partitionDesc, maxPartitionTokensPQ, ring.partitionsByToken)
		}
	}

	return NewPartitionRingWithPartitionsByToken(desc, ring.partitionsByToken)
}

func (s *preserveConsistencyPartitionRingTokenShuffler) updateTokensFromPQOld(partitionDesc *PartitionDesc, tokenPQ ownershipPriorityQueue[ringInstance]) {
	tokens := partitionDesc.GetTokens()
	if tokens == nil {
		tokens = make(Tokens, optimalTokensPerInstance)
	}
	for i := 0; i < len(tokens); i++ {
		token := heap.Pop(&tokenPQ).(ownershipInfo[ringInstance])
		tokens[i] = uint32(token.item.instanceID)
	}
	partitionDesc.Tokens = tokens
}

func (s *preserveConsistencyPartitionRingTokenShuffler) updateTokensFromPQ(partitionDesc *PartitionDesc, tokenPQ ownershipPriorityQueue[ringInstance], partitionsByToken map[Token]*list.List) {
	tokens := partitionDesc.GetTokens()
	if tokens == nil {
		tokens = make(Tokens, optimalTokensPerInstance)
	}
	for i := 0; i < len(tokens); i++ {
		token := heap.Pop(&tokenPQ).(ownershipInfo[ringInstance])
		tokens[i] = uint32(token.item.instanceID)
	}
	partitionDesc.Tokens = tokens

	now := time.Now().Unix()
	for _, token := range tokens {
		partitions := partitionsByToken[Token(token)]
		oldPartition := partitions.Front().Value.(*partition)
		oldPartition.validUntil = now
		partitions.PushFront(&partition{id: partitionDesc.Id})
	}
}
