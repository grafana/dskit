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
	require.Equal(t, workerCount, countGoroutines())

	// Wait a little bit so both goroutines would be waiting on the jobs chan.
	time.Sleep(10 * time.Millisecond)

	ch := make(chan struct{})
	w.Go(func() { <-ch })
	require.Equal(t, workerCount, countGoroutines())
	w.Go(func() { <-ch })
	require.Equal(t, workerCount, countGoroutines())
	w.Go(func() { <-ch })
	require.Equal(t, workerCount+1, countGoroutines())

	// end workloads, we should have only the workers again.
	close(ch)
	for i := 0; i < 1000; i++ {
		if countGoroutines() == workerCount {
			break
		}
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

func TestReusableGoroutinesPool_ClosedActionPanic(t *testing.T) {
	w := NewReusableGoroutinesPool(2)

	runCount, panicked, _ := causePoolFailure(t, w, 10)

	require.NotZero(t, runCount, "expected at least one run")
	require.Less(t, runCount, 10, "expected less than 10 runs")
	require.True(t, panicked, "expected panic")
}

func TestReusableGoroutinesPool_ClosedActionError(t *testing.T) {
	w := NewReusableGoroutinesPool(2).WithClosedAction(ErrorWhenClosed)

	runCount, panicked, errors := causePoolFailure(t, w, 10)

	require.NotZero(t, runCount, "expected at least one run")
	require.Less(t, runCount, 10, "expected less than 10 runs")
	require.False(t, panicked, "expected no panic")
	require.NotZero(t, len(errors), "expected errors")
	require.Less(t, len(errors), 10, "expected less than 10 errors. Some workloads were submitted before close.")
}

func TestReusableGoroutinesPool_ClosedActionSpawn(t *testing.T) {
	w := NewReusableGoroutinesPool(2).WithClosedAction(SpawnNewGoroutineWhenClosed)

	runCount, panicked, errors := causePoolFailure(t, w, 10)

	require.Equal(t, runCount, 10, "expected all workloads to run")
	require.False(t, panicked, "expected no panic")
	require.NotZero(t, len(errors), "expected errors")
	require.Less(t, len(errors), 10, "expected less than 10 errors. Some workloads were submitted before close.")
}

func causePoolFailure(t *testing.T, w *ReusableGoroutinesPool, maxMsgCount int) (runCount int, panicked bool, errors []error) {
	t.Helper()

	var runCountAtomic atomic.Int32

	var testWG sync.WaitGroup
	testWG.Add(1)
	go func() {
		defer testWG.Done()
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		for i := 0; i < maxMsgCount; i++ {
			err := w.GoErr(func() {
				runCountAtomic.Add(1)
			})
			if err != nil {
				errors = append(errors, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	time.Sleep(10 * time.Millisecond)
	w.Close()     // close the pool
	testWG.Wait() // wait for the test to finish

	return int(runCountAtomic.Load()), panicked, errors
}
