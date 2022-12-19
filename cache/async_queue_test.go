package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAsyncQueue_SequentialRun(t *testing.T) {
	q := newAsyncQueue(10, 10)
	defer q.stop()

	var i int
	_ = q.run(func() { i += 1 })
	_ = q.run(func() { i -= 1 })
	_ = q.run(func() { i += 1 })
	_ = q.run(func() { i -= 1 })
	_ = q.run(func() { i += 1 })

	// Wait for all operations to finish.
	time.Sleep(100 * time.Millisecond)

	require.Equal(t, 1, i)
}

func TestAsyncQueue_QueueFullError(t *testing.T) {
	const queueLength = 2

	q := newAsyncQueue(queueLength, 1)
	defer q.stop()

	doneCh := make(chan struct{})
	defer func() {
		close(doneCh)
	}()

	// Keep worker busy.
	_ = q.run(func() {
		<-doneCh
	})
	time.Sleep(100 * time.Millisecond)

	// Fill the queue.
	for i := 0; i < queueLength; i++ {
		require.NoError(t, q.run(func() {}))
	}
	require.Equal(t, errAsyncQueueFull, q.run(func() {}))
}
