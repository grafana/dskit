package concurrency

import (
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

	baseGoroutines := countGoroutines()
	const workers = 2
	w := NewReusableGoroutinesPool(workers)

	require.Equal(t, baseGoroutines+workers, countGoroutines())

	// Wait a little bit so both goroutines would be waiting on the jobs chan.
	time.Sleep(10 * time.Millisecond)

	ch := make(chan struct{})
	w.Go(func() { <-ch })
	require.Equal(t, baseGoroutines+workers, countGoroutines())
	w.Go(func() { <-ch })
	require.Equal(t, baseGoroutines+workers, countGoroutines())
	w.Go(func() { <-ch })
	require.Equal(t, baseGoroutines+workers+1, countGoroutines())

	// end workloads, we should have only the workers again.
	close(ch)
	for i := 0; i < 1000; i++ {
		if countGoroutines() == baseGoroutines+workers {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, baseGoroutines+workers, countGoroutines())

	// close the workers, eventually they should be gone.
	w.Close()
	for i := 0; i < 1000; i++ {
		if countGoroutines() == baseGoroutines {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected %d goroutines after closing, got %d", baseGoroutines, countGoroutines())
}
