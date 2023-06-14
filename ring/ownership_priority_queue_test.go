package ring

import (
	"container/heap"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	minTokenWeight = 100
	maxTokenWeight = 1000
)

func getRandomTokenWeight() float64 {
	return (maxTokenWeight-minTokenWeight)*rand.Float64() + minTokenWeight
}

func createPriorityQueueWithoutInitialization(size int) (*ownershipPriorityQueue[*ringInstance], float64, float64) {
	pq := newPriorityQueue[*ringInstance](size)

	minWeight := float64(math.MaxUint32)
	maxWeight := 0.0
	for i := 1; i <= size-1; i++ {
		randomWeight := getRandomTokenWeight()
		if randomWeight > maxWeight {
			maxWeight = randomWeight
		}
		if randomWeight < minWeight {
			minWeight = randomWeight
		}
		item := newRingInstanceOwnershipInfo(rand.Int(), randomWeight)
		pq.Add(item)
	}
	return pq, minWeight, maxWeight
}

func TestOwnershipPriorityQueue_EqualOwnershipOfRingTokens(t *testing.T) {
	pq := newPriorityQueue[*ringToken](3)
	// ownership of first is 20, and its token is 60
	first := newRingTokenOwnershipInfo(60, 40)
	pq.Add(first)
	// ownership of second is 20, and its token is 40, so second > first
	// (when ownership is equal, priority is decided by the order of ids)
	second := newRingTokenOwnershipInfo(40, 40)
	pq.Add(second)
	// ownership of third is 10, so second > first > third
	third := newRingTokenOwnershipInfo(20, 10)
	pq.Add(third)
	heap.Init(pq)

	max := heap.Pop(pq).(*ownershipInfo[*ringToken])
	require.Equal(t, second, max)

	max = heap.Pop(pq).(*ownershipInfo[*ringToken])
	require.Equal(t, first, max)

	max = heap.Pop(pq).(*ownershipInfo[*ringToken])
	require.Equal(t, third, max)
}

func TestOwnershipPriorityQueue_EqualOwnershipOfRingInstances(t *testing.T) {
	pq := newPriorityQueue[*ringInstance](3)
	// ownership of first is 40.0, and its id is 10
	first := newRingInstanceOwnershipInfo(10, 40.0)
	pq.Add(first)
	// ownership of second is 40.0, and its id is 8, so first > second
	// (when ownership is equal, priority is decided by comparing ids)
	second := newRingInstanceOwnershipInfo(8, 40.0)
	pq.Add(second)
	// ownership of third is 15.0, so first > second > third
	third := newRingInstanceOwnershipInfo(9, 15.0)
	pq.Add(third)
	heap.Init(pq)

	max := heap.Pop(pq).(*ownershipInfo[*ringInstance])
	require.Equal(t, first, max)

	max = heap.Pop(pq).(*ownershipInfo[*ringInstance])
	require.Equal(t, second, max)

	max = heap.Pop(pq).(*ownershipInfo[*ringInstance])
	require.Equal(t, third, max)
}

func TestOwnershipPriorityQueue_PushPopPeek(t *testing.T) {
	size := 10
	pq, minWeight, maxWeight := createPriorityQueueWithoutInitialization(size)

	// Initialize PriorityQueue
	heap.Init(pq)

	// Check that the highest priority is maxWeight, but don't remove it
	require.Equal(t, maxWeight, pq.Peek().ownership)

	newMaxWeight := maxWeight + 1.0
	// Push to pq an element with the priority higher than the current maximal priority
	oi := newRingInstanceOwnershipInfo(11, newMaxWeight)
	heap.Push(pq, oi)

	// Check that the highest priority is now newMaxWeight, but don't remove it
	require.Equal(t, newMaxWeight, pq.Peek().ownership)

	// Push to pq an element with the priority lower than the current minimal priority
	newMinWeight := minWeight - 1.0
	oi = newRingInstanceOwnershipInfo(12, newMinWeight)
	heap.Push(pq, oi)

	// Check that the maximal priority is newMaxWeight and remove it
	item := heap.Pop(pq).(*ownershipInfo[*ringInstance])
	require.Equal(t, newMaxWeight, item.ownership)

	// Check that the highest priority is again maxWeight, but don't remove it
	require.Equal(t, maxWeight, pq.Peek().ownership)

	// Check that all other elements except the last one are sorted correctly
	currWeight := math.MaxFloat64
	for pq.Len() > 1 {
		weightedNavigableToken := heap.Pop(pq).(*ownershipInfo[*ringInstance])
		require.Less(t, weightedNavigableToken.ownership, currWeight)
		currWeight = weightedNavigableToken.ownership
	}

	// Check that the minimal priority is newMinWeight
	item = heap.Pop(pq).(*ownershipInfo[*ringInstance])
	require.Equal(t, newMinWeight, item.ownership)
}

func TestOwnershipPriorityQueue_Update(t *testing.T) {
	first := newRingInstanceOwnershipInfo(1, 3.0)
	second := newRingInstanceOwnershipInfo(2, 5.0)
	third := newRingInstanceOwnershipInfo(3, 4.0)

	pq := newPriorityQueue[*ringInstance](3)
	pq.Add(first)
	pq.Add(second)
	pq.Add(third)

	heap.Init(pq)

	// Check that second has the highest priority
	require.Equal(t, second, pq.Peek())

	// Update the value of first and assign it the highest priority
	pq.Update(first, func(oi *ownershipInfo[*ringInstance]) {
		oi.ownership *= 2
	})

	// Check that now first has the highest priority
	require.Equal(t, first, pq.Peek())
}
