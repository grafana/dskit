package concurrency

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReusableGoroutinesPool(t *testing.T) {
	baseGoroutines := runtime.NumGoroutine()
	const workers = 2

	w := NewReusableGoroutinesPool(workers)
	require.Equal(t, baseGoroutines+workers, runtime.NumGoroutine())

	// Wait a little bit so both goroutines would be waiting on the jobs chan.
	time.Sleep(10 * time.Millisecond)

	ch := make(chan struct{})
	w.Go(func() { <-ch })
	require.Equal(t, baseGoroutines+workers, runtime.NumGoroutine())
	w.Go(func() { <-ch })
	require.Equal(t, baseGoroutines+workers, runtime.NumGoroutine())
	w.Go(func() { <-ch })
	require.Equal(t, baseGoroutines+workers+1, runtime.NumGoroutine())

	// end workloads, we should have only the workers again.
	close(ch)
	for i := 0; i < 1000; i++ {
		if runtime.NumGoroutine() == baseGoroutines+workers {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, baseGoroutines+workers, runtime.NumGoroutine())

	// close the workers, eventually they should be gone.
	w.Close()
	for i := 0; i < 1000; i++ {
		if runtime.NumGoroutine() == baseGoroutines {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected %d goroutines after closing, got %d", baseGoroutines, runtime.NumGoroutine())
}
