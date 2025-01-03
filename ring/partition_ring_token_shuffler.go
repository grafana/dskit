package ring

import (
	"container/heap"
	"fmt"
	"math"
	"strings"
)

type partitionRingTokenShuffler interface {
	// shuffle gets a PartitionRing, and a map of timeseries ownership by token,
	// and builds a new PartitionRing with the same partitions and tokens as the original
	// ring, but possibly with a different distribution of tokens among partitions.
	shuffle(PartitionRing, map[Token]float64, bool) *PartitionRing
}

type preserveConsistencyPartitionRingTokenShuffler struct {
	partitionRingTokenShuffler
}

func (s *preserveConsistencyPartitionRingTokenShuffler) minMaxOwnership(partitionByToken map[Token]int32, timeseriesOwnershipByToken map[Token]float64) (int32, float64, int32, float64) {
	timeseriesOwnershipByPartition := make(map[int32]float64)
	for token := range partitionByToken {
		partition := partitionByToken[token]
		timeseriesOwnership := timeseriesOwnershipByToken[token]
		timeseriesOwnershipByPartition[partition] = timeseriesOwnershipByPartition[partition] + timeseriesOwnership
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

	return minPartition, minTimeseriesOwnership, maxPartition, maxTimeseriesOwnership
}

func (s *preserveConsistencyPartitionRingTokenShuffler) shuffle(ring PartitionRing, timeseriesOwnershipByToken map[Token]float64, printSteps bool) *PartitionRing {
	minPartition, minTimeseriesOwnership, maxPartition, maxTimeseriesOwnership := s.minMaxOwnership(ring.partitionByToken, timeseriesOwnershipByToken)

	if printSteps {
		fmt.Printf("Partitions with min and max timeseries ownership are %d (%10.3f) and %d (%10.3f)\n", minPartition, minTimeseriesOwnership, maxPartition, maxTimeseriesOwnership)
	}

	maxPartitionTokensPQ := newPriorityQueue[ringInstance](optimalTokensPerInstance, true)
	minPartitionTokensPQ := newPriorityQueue[ringInstance](optimalTokensPerInstance, false)

	for token, partition := range ring.partitionByToken {
		timeseriesOwnership := timeseriesOwnershipByToken[token]
		if partition == minPartition {
			item := newRingInstanceOwnershipInfo(int(token), timeseriesOwnership)
			heap.Push(&minPartitionTokensPQ, item)
		} else if partition == maxPartition {
			item := newRingInstanceOwnershipInfo(int(token), timeseriesOwnership)
			heap.Push(&maxPartitionTokensPQ, item)
		}
	}

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

	for partition, partitionDesc := range desc.Partitions {
		if partition == minPartition {
			s.updateTokensFromPQ(&partitionDesc, minPartitionTokensPQ)
		} else if partition == maxPartition {
			s.updateTokensFromPQ(&partitionDesc, maxPartitionTokensPQ)
		}
	}

	return NewPartitionRing(desc)
}

func (s *preserveConsistencyPartitionRingTokenShuffler) updateTokensFromPQ(partitionDesc *PartitionDesc, tokenPQ ownershipPriorityQueue[ringInstance]) {
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
