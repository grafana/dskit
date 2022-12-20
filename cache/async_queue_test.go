package cache

import (
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/stretchr/testify/require"
)

func TestAsyncQueue_Run(t *testing.T) {
	q := newAsyncQueue(10, 10)
	defer q.stop()

	var i atomic.Int32
	_ = q.submit(func() { i.Add(1) })
	_ = q.submit(func() { i.Add(-1) })
	_ = q.submit(func() { i.Add(1) })
	_ = q.submit(func() { i.Add(-1) })
	_ = q.submit(func() { i.Add(1) })

	// Wait for all operations to finish.
	time.Sleep(100 * time.Millisecond)

	require.Equal(t, int32(1), i.Load())
}

func TestAsyncQueue_QueueFullError(t *testing.T) {
	const queueLength = 10

	q := newAsyncQueue(queueLength, 1)
	defer q.stop()

	doneCh := make(chan struct{})
	defer func() {
		close(doneCh)
	}()

	// Keep worker busy.
	_ = q.submit(func() {
		<-doneCh
	})
	time.Sleep(100 * time.Millisecond)

	// Fill the queue.
	for i := 0; i < queueLength; i++ {
		require.NoError(t, q.submit(func() {}))
	}
	require.Equal(t, errAsyncQueueFull, q.submit(func() {}))
}
