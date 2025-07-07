package concurrency

import (
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestReusableGoroutinesPool(t *testing.T) {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, false)]
	testGoroutine := regexp.MustCompile(`goroutine (\d+) \[running\]:`).FindSubmatch(buf)[1]
	require.NotEmpty(t, testGoroutine, "test goroutine not found")

	countGoroutines := func() int {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		// Count the number of goroutines created by this test
		// This ensures that the test isn't affected by leaked goroutines from other tests.
		return strings.Count(string(buf), " in goroutine "+string(testGoroutine))
	}

	const workerCount = 2
	w := NewReusableGoroutinesPool(workerCount)
	require.Equal(t, 0, countGoroutines())

	// Wait a little bit so both goroutines would be waiting on the jobs chan.
	time.Sleep(10 * time.Millisecond)

	ch := make(chan struct{})
	w.Go(func() { <-ch })
	require.Equal(t, 1, countGoroutines())
	w.Go(func() { <-ch })
	require.Equal(t, workerCount, countGoroutines())
	w.Go(func() { <-ch })
	require.Equal(t, workerCount+1, countGoroutines())

	// end workloads, we should have only the workers again.
	close(ch)
	for i := 0; i < 1000 && countGoroutines() >= workerCount; i++ {
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, workerCount, countGoroutines())

	// close the workers, eventually they should be gone.
	w.Close()
	for i := 0; i < 1000; i++ {
		if countGoroutines() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected %d goroutines after closing, got %d", 0, countGoroutines())
}

// TestReusableGoroutinesPool_Race tests that Close() and Go() can be called concurrently.
func TestReusableGoroutinesPool_Race(t *testing.T) {
	w := NewReusableGoroutinesPool(2)

	var runCountAtomic atomic.Int32
	const maxMsgCount = 10

	var testWG sync.WaitGroup
	testWG.Add(1)
	go func() {
		defer testWG.Done()
		for i := 0; i < maxMsgCount; i++ {
			w.Go(func() {
				runCountAtomic.Add(1)
			})
			time.Sleep(10 * time.Millisecond)
		}
	}()
	time.Sleep(10 * time.Millisecond)
	w.Close()     // close the pool
	testWG.Wait() // wait for the test to finish

	runCt := int(runCountAtomic.Load())
	require.Equal(t, runCt, 10, "expected all functions to run")
}
