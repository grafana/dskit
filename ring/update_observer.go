package ring

import (
	"context"
	"time"

	"go.uber.org/atomic"
)

type updateObserver[T any] interface {
	observeUpdate(*T)
}

// delayedObserver is an observer that waits for a certain interval before sending
// the update to the receiver. This is useful when the updates are frequent and
// we only care to observe the latest one.
type delayedObserver[T any] struct {
	value    atomic.Pointer[T]
	receiver func(*T)
	interval time.Duration
}

func newDelayedObserver[T any](interval time.Duration, receiver func(*T)) *delayedObserver[T] {
	if interval <= 0 {
		panic("newDelayedObserver: interval must be greater than 0")
	}
	return &delayedObserver[T]{
		receiver: receiver,
		interval: interval,
	}
}

// observeUpdate stores an updated value for a later flush.
func (w *delayedObserver[T]) observeUpdate(u *T) {
	w.value.Store(u)
}

func (w *delayedObserver[T]) run(ctx context.Context) {
	go func() {
		t := time.NewTicker(w.interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				w.flush()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// flush sends the update to the receiver if there is one.
func (w *delayedObserver[T]) flush() {
	if v := w.value.Swap(nil); v != nil {
		w.receiver(v)
	}
}

var _ updateObserver[int] = &delayedObserver[int]{}

// noDelayObserver is an observer that synchronously sends the update to the
// receiver.
type noDelayObserver[T any] struct {
	receiver func(*T)
}

func newNoDelayObserver[T any](receiver func(*T)) *noDelayObserver[T] {
	return &noDelayObserver[T]{
		receiver: receiver,
	}
}

// observeUpdate sends the update to the receiver immediately.
func (w *noDelayObserver[T]) observeUpdate(u *T) {
	w.receiver(u)
}

var _ updateObserver[int] = &noDelayObserver[int]{}
