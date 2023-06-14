package ring

import (
	"container/heap"
	"fmt"
	"math"
	"strings"
)

type ringItem interface {
	GetID() int
	Less(other ringItem) bool
}

type ringInstance struct {
	instanceID int
}

func newRingInstance(instanceID int) *ringInstance {
	return &ringInstance{
		instanceID: instanceID,
	}
}

func (ri *ringInstance) GetID() int {
	return ri.instanceID
}

func (ri *ringInstance) Less(other ringItem) bool {
	ringInstance, ok := other.(*ringInstance)
	if !ok {
		return false
	}
	return ri.instanceID < ringInstance.instanceID
}

func (ri *ringInstance) String() string {
	return fmt.Sprintf("[instanceID: %d]", ri.instanceID)
}

type ringToken struct {
	token     uint32
	prevToken uint32
}

func newRingToken(token, prevToken uint32) *ringToken {
	return &ringToken{
		token:     token,
		prevToken: prevToken,
	}
}

func (rt *ringToken) GetID() int {
	return int(rt.token)
}

func (rt *ringToken) Less(other ringItem) bool {
	ringToken, ok := other.(*ringToken)
	if !ok {
		return false
	}
	return rt.token < ringToken.token
}

func (rt *ringToken) String() string {
	return fmt.Sprintf("[token: %d, prevToken: %d]", rt.token, rt.prevToken)
}

type ownershipInfo struct {
	ringItem  ringItem
	ownership float64
	index     int
}

func newRingTokenOwnershipInfo(token, prevToken uint32) *ownershipInfo {
	rT := newRingToken(token, prevToken)
	ownership := float64(getTokenDistance(prevToken, token))
	return &ownershipInfo{
		ownership: ownership,
		ringItem:  rT,
	}
}

func newRingInstanceOwnershipInfo(instanceID int, ownership float64) *ownershipInfo {
	rI := newRingInstance(instanceID)
	return &ownershipInfo{
		ownership: ownership,
		ringItem:  rI,
	}
}

// ownershipPriorityQueue is a max-heap, i.e., a priority queue
// where items with a higher priority will be extracted first.
// Namely, items with a higher ownership have a higher priority.
// In order to guarantee that 2 instances of ownershipPriorityQueue
// with the same items always assign equal priorities to equal items,
// in the case of items with equal ownership, we rely on the
// order of item ids.
type ownershipPriorityQueue struct {
	items []*ownershipInfo
}

func newPriorityQueue(len int) *ownershipPriorityQueue {
	return &ownershipPriorityQueue{
		items: make([]*ownershipInfo, 0, len),
	}
}

func (pq *ownershipPriorityQueue) Len() int {
	return len(pq.items)
}

func (pq *ownershipPriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i // pq[i] is actually the old pq[j], so pq[i].index should be updated to i
	pq.items[j].index = j // pq[j] is actually the old pq[i], so pq[j].index should be updated to j
}

func (pq *ownershipPriorityQueue) Less(i, j int) bool {
	if pq.items[i].ownership == pq.items[j].ownership {
		// In order to guarantee the stability, i.e., that the same instanceID and zone as input
		// always generate the same slice of tokens as output, we enforce that by equal ownership
		// higher priority is determined by the order of ids.
		return pq.compareRingItems(pq.items[i].ringItem, pq.items[j].ringItem)
	}
	// We are implementing a max-heap, so we are using > here.
	// Since we compare float64, NaN values must be placed at the end.
	return pq.items[i].ownership > pq.items[j].ownership || (math.IsNaN(pq.items[j].ownership) && !math.IsNaN(pq.items[i].ownership))
}

func (pq *ownershipPriorityQueue) compareRingItems(ri1, ri2 ringItem) bool {
	// we invert the order because we want that instances with higher ids have a higher priority
	return ri2.Less(ri1)
}

// Push implements heap.Push(any). It pushes the element item onto ownershipPriorityQueue.
// Time complexity is O(log n), where n = Len().
func (pq *ownershipPriorityQueue) Push(item any) {
	n := len(pq.items)
	ownershipInfo := item.(*ownershipInfo)
	ownershipInfo.index = n
	pq.items = append(pq.items, ownershipInfo)
}

// Pop implements heap.Pop(). It removes and returns the element with the highest priority from ownershipPriorityQueue.
// Time complexity is O(log n), where n = Len().
func (pq *ownershipPriorityQueue) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	pq.items = old[0 : n-1]
	return item
}

// Peek the returns the element with the highest priority from ownershipPriorityQueue,
// but it does not remove it from the latter. Time complexity is O(1).
func (pq *ownershipPriorityQueue) Peek() *ownershipInfo {
	return (pq.items)[0]
}

// Update updates the element ownershipInfo passed as parameter by applying to it the updating function update
// passed as parameter, and propagates this modification to ownershipPriorityQueue. Element ownershipInfo must
// be already present on ownershipPriorityQueue. Time complexity is O(log n), where n = Len().
func (pq *ownershipPriorityQueue) Update(ownershipInfo *ownershipInfo, update func(*ownershipInfo)) {
	update(ownershipInfo)
	heap.Fix(pq, ownershipInfo.index)
}

// Add adds an element at the end of the queue, but it does not take into account the ownership value.
// In order to re-stabilize the priority queue property it is necessary to call heap.Init() on this queue.
func (pq *ownershipPriorityQueue) Add(ownershipInfo *ownershipInfo) {
	pq.items = append(pq.items, ownershipInfo)
}

// Clear removes all the items from the queue.
func (pq *ownershipPriorityQueue) Clear() {
	if len(pq.items) != 0 {
		pq.items = pq.items[:0]
	}
}

func (pq *ownershipPriorityQueue) String() string {
	return fmt.Sprintf("[%s]", strings.Join(mapItems(pq.items, func(item *ownershipInfo) string {
		return fmt.Sprintf("%s-ownership: %.3f", item.ringItem, item.ownership)
	}), ","))
}

func mapItems[T, V any](in []T, mapItem func(T) V) []V {
	out := make([]V, len(in))
	for i, v := range in {
		out[i] = mapItem(v)
	}
	return out
}
