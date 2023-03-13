package cache

import (
	"container/list"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/atomic"
)

var errAsyncQueueFull = errors.New("the async queue is full")

type enqueuedOp struct {
	size int
	do   func()
}

type asyncQueue struct {
	queueSignal chan struct{}

	queueM             *sync.Mutex
	queueSpareCapacity *atomic.Uint64
	queue              *list.List

	workers sync.WaitGroup
}

func newAsyncQueue(length, maxConcurrency int) *asyncQueue {
	q := &asyncQueue{
		// Buffer one element so that we can avoid a race condition where a processor goroutine
		// * acquires the lock for the queue
		// * checks the size of the queue, it's 0
		// * releases the lock
		// * a client inserts an element in the queue
		// * the client tries to send a signal in queueSignal, but can't since no goroutine is waiting on the channel
		// * the processor goroutine starts waiting on queueSignal
		// In that case the processor goroutine misses the signal. If there is always at least one element in queueSignal,
		// then we trade missed signals for false-positive signals. With false-positive signals we only need to check if
		// the queue actually has an item.
		queueSignal:        make(chan struct{}, 1),
		queueM:             &sync.Mutex{},
		queueSpareCapacity: atomic.NewUint64(uint64(length)),
		queue:              list.New(),
	}
	// Start a number of goroutines - processing async operations - equal
	// to the max concurrency we have.
	q.workers.Add(maxConcurrency)
	for i := 0; i < maxConcurrency; i++ {
		go q.asyncQueueProcessLoop()
	}
	return q
}

func (q *asyncQueue) submit(op func()) error {
	return q.submitWeight(op, 1)
}

func (q *asyncQueue) submitWeight(op func(), weight int) error {
	if q.queueSpareCapacity.Sub(uint64(weight)) < 0 {
		q.queueSpareCapacity.Add(uint64(weight))
		return errAsyncQueueFull
	}

	q.queueM.Lock()
	defer q.queueM.Unlock()

	q.queue.PushBack(enqueuedOp{do: op})
	select {
	case q.queueSignal <- struct{}{}:
		// Try signalling to the goroutines that there's an element. If they're all busy, insert one signal in the channel
		// to avoid a race condition and missing this element once a goroutine is free.
		// If the channel is already full, then there is already at least another element waiting in the queue.
	default:
	}
	return nil
}

func (q *asyncQueue) stop() {
	close(q.queueSignal)
	q.workers.Wait()
}

func (q *asyncQueue) asyncQueueProcessLoop() {
	defer q.workers.Done()

	for {
		q.queueM.Lock()
		if q.queue.Len() == 0 {
			q.queueM.Unlock()
			// The queue is empty, wait for a signal that an item has been added.
			_, ok := <-q.queueSignal
			if !ok {
				// If the queue signals are closed, then we're shutting down.
				return
			}
			q.queueM.Lock()
			if q.queue.Len() == 0 {
				// It's possible that the signal was sent but another goroutine consumed the element without waiting on the channel first.
				// This was a ghost signal, and we can safely ignore it.
				q.queueM.Unlock()
				continue
			}
		}
		// At this point we know the queue isn't empty. We can pop an item and process it.
		frontElem := q.queue.Front()
		q.queue.Remove(frontElem)
		q.queueM.Unlock()

		op := frontElem.Value.(enqueuedOp)
		op.do()
		q.queueSpareCapacity.Add(uint64(op.size)) // Do the weight subtraction after the operation so that the total weight includes the items in flight.
	}
}
