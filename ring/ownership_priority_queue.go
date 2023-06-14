package ring

import (
	"fmt"
	"math"
	"strings"
)

type ringItem interface {
	key() int
	String() string
}

type ringInstance struct {
	instanceID int
}

func (ri ringInstance) key() int {
	return ri.instanceID
}

func (ri ringInstance) String() string {
	return fmt.Sprintf("[instanceID: %d]", ri.instanceID)
}

type ringToken struct {
	token     uint32
	prevToken uint32
}

func (rt ringToken) key() int {
	return int(rt.token)
}

func (rt ringToken) String() string {
	return fmt.Sprintf("[token: %d, prevToken: %d]", rt.token, rt.prevToken)
}

type ownershipInfo[T ringItem] struct {
	item      T
	ownership float64
}

func newRingTokenOwnershipInfo(token, prevToken uint32) ownershipInfo[ringToken] {
	ownership := float64(getTokenDistance(prevToken, token))
	return ownershipInfo[ringToken]{
		ownership: ownership,
		item: ringToken{
			token:     token,
			prevToken: prevToken,
		},
	}
}

func newRingInstanceOwnershipInfo(instanceID int, ownership float64) ownershipInfo[ringInstance] {
	return ownershipInfo[ringInstance]{
		ownership: ownership,
		item: ringInstance{
			instanceID: instanceID,
		},
	}
}

// ownershipPriorityQueue is a max-heap, i.e., a priority queue
// where items with a higher priority will be extracted first.
// Namely, items with a higher ownership have a higher priority.
// In order to guarantee that 2 instances of ownershipPriorityQueue
// with the same items always assign equal priorities to equal items,
// in the case of items with equal ownership, we rely on the
// order of item ids.
type ownershipPriorityQueue[T ringItem] struct {
	items []ownershipInfo[T]
}

func newPriorityQueue[T ringItem](len int) ownershipPriorityQueue[T] {
	return ownershipPriorityQueue[T]{
		items: make([]ownershipInfo[T], 0, len),
	}
}

func (pq *ownershipPriorityQueue[T]) Len() int {
	return len(pq.items)
}

func (pq *ownershipPriorityQueue[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *ownershipPriorityQueue[T]) Less(i, j int) bool {
	if pq.items[i].ownership == pq.items[j].ownership {
		// In order to guarantee the stability, i.e., that the same instanceID and zone as input
		// always generate the same slice of tokens as output, we enforce that by equal ownership
		// higher priority is determined by the order of ids.
		return pq.items[i].item.key() > pq.items[j].item.key()
	}
	// We are implementing a max-heap, so we are using > here.
	// Since we compare float64, NaN values must be placed at the end.
	return pq.items[i].ownership > pq.items[j].ownership || (math.IsNaN(pq.items[j].ownership) && !math.IsNaN(pq.items[i].ownership))
}

// Push implements heap.Push(any). It pushes the element item onto ownershipPriorityQueue.
// Time complexity is O(log n), where n = Len().
func (pq *ownershipPriorityQueue[T]) Push(item any) {
	ownershipInfo := item.(ownershipInfo[T])
	pq.items = append(pq.items, ownershipInfo)
}

// Pop implements heap.Pop(). It removes and returns the element with the highest priority from ownershipPriorityQueue.
// Time complexity is O(log n), where n = Len().
func (pq *ownershipPriorityQueue[T]) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	pq.items = old[0 : n-1]
	return item
}

// Peek the returns the element with the highest priority from ownershipPriorityQueue,
// but it does not remove it from the latter. Time complexity is O(1).
func (pq *ownershipPriorityQueue[T]) Peek() *ownershipInfo[T] {
	return &pq.items[0]
}

// Add adds an element at the end of the queue, but it does not take into account the ownership value.
// In order to re-stabilize the priority queue property it is necessary to call heap.Init() on this queue.
func (pq *ownershipPriorityQueue[T]) Add(ownershipInfo ownershipInfo[T]) {
	pq.items = append(pq.items, ownershipInfo)
}

// Clear removes all the items from the queue.
func (pq *ownershipPriorityQueue[T]) Clear() {
	if len(pq.items) != 0 {
		pq.items = pq.items[:0]
	}
}

func (pq *ownershipPriorityQueue[T]) String() string {
	return fmt.Sprintf("[%s]", strings.Join(mapItems(pq.items, func(item ownershipInfo[T]) string {
		return fmt.Sprintf("%s-ownership: %.3f", item.item, item.ownership)
	}), ","))
}

func mapItems[T, V any](in []T, mapItem func(T) V) []V {
	out := make([]V, len(in))
	for i, v := range in {
		out[i] = mapItem(v)
	}
	return out
}
