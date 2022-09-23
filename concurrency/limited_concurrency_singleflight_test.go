package concurrency

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestLimitedConcurrencySingleFlight_ForEachNotInFlight_ConcurrencyLimit(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pool           = NewWorkerPool(10)
		workersWait    = make(chan struct{})
		workersToStart sync.WaitGroup
	)

	// Wait for the pool so we don't leak goroutines
	t.Cleanup(pool.Wait)
	t.Cleanup(func() { close(workersWait) })

	forEachNotInFlight := func(tokens []int, f func(context.Context, string) error) {
		stringTokens := make([]string, len(tokens))
		for i, token := range tokens {
			stringTokens[i] = strconv.Itoa(token)
		}
		require.NoError(t, pool.ForEachNotInFlight(ctx, stringTokens, f))
	}

	busyWorker := func(ctx context.Context, s string) error {
		workersToStart.Done()
		<-workersWait
		return nil
	}

	workersToStart.Add(10)
	go forEachNotInFlight([]int{0, 1}, busyWorker)
	go forEachNotInFlight([]int{2, 3}, busyWorker)
	go forEachNotInFlight([]int{4, 5}, busyWorker)
	go forEachNotInFlight([]int{6, 7}, busyWorker)
	go forEachNotInFlight([]int{8, 9}, busyWorker)
	workersToStart.Wait()

	extraWorkerInvoked := make(chan struct{})
	go forEachNotInFlight([]int{10}, func(ctx context.Context, s string) error {
		close(extraWorkerInvoked)
		return nil
	})

	select {
	case <-extraWorkerInvoked:
		t.Error("the extra worker was invoked even though the concurrency limit was maxed out")
	case <-time.After(time.Second):
	}
}

func TestLimitedConcurrencySingleFlight_ForEachNotInFlight_ReturnsWhenAllTokensAreInflight(t *testing.T) {
	const token = "t"

	var (
		ctx            = context.Background()
		pool           = NewWorkerPool(10)
		workersWait    = make(chan struct{})
		workersToStart sync.WaitGroup
	)

	// Wait for the pool so we don't leak goroutines
	t.Cleanup(pool.Wait)
	t.Cleanup(func() { close(workersWait) })

	workersToStart.Add(1)
	go func() {
		require.NoError(t, pool.ForEachNotInFlight(ctx, []string{token}, func(ctx context.Context, s string) error {
			workersToStart.Done()
			<-workersWait
			return nil
		}))
	}()

	workersToStart.Wait()

	duplicatedTokenInvoked := false
	require.NoError(t, pool.ForEachNotInFlight(ctx, []string{token}, func(ctx context.Context, s string) error {
		duplicatedTokenInvoked = true
		return nil
	}))

	assert.False(t, duplicatedTokenInvoked, "the same token was invoked again while there is an in-flight invocation with it")
}

func TestLimitedConcurrencySingleFlight_ForEachNotInFlight_CallsOnlyNotInFlightTokens(t *testing.T) {
	const (
		tokenA = "a"
		tokenB = "b"
	)

	var (
		ctx            = context.Background()
		pool           = NewWorkerPool(10)
		workersWait    = make(chan struct{})
		workersToStart sync.WaitGroup
	)

	// Wait for the pool so we don't leak goroutines
	t.Cleanup(pool.Wait)
	t.Cleanup(func() { close(workersWait) })

	workersToStart.Add(1)
	go func() {
		require.NoError(t, pool.ForEachNotInFlight(ctx, []string{tokenA}, func(ctx context.Context, s string) error {
			workersToStart.Done()
			<-workersWait
			return nil
		}))
	}()

	workersToStart.Wait()
	var invocations atomic.Int64
	assert.NoError(t, pool.ForEachNotInFlight(ctx, []string{tokenA, tokenB}, func(ctx context.Context, s string) error {
		assert.Equal(t, tokenB, s)
		invocations.Inc()
		return nil
	}))

	assert.Equal(t, int64(1), invocations.Load(), "the same tokenA was invoked again while there is an in-flight invocation with it")
}

func TestLimitedConcurrencySingleFlight_ForEachNotInFlight_ReturnsWhenTokensAreEmpty(t *testing.T) {
	t.Parallel()

	var invocations atomic.Int64
	assert.NoError(t, NewWorkerPool(10).ForEachNotInFlight(context.Background(), []string{}, func(ctx context.Context, s string) error {
		invocations.Inc()
		return nil
	}))

	assert.Zero(t, invocations.Load())
}

func TestLimitedConcurrencySingleFlight_ForEachNotInFlight_CancelledContext(t *testing.T) {
	t.Parallel()

	var (
		tokens         = []string{"t1", "t2"}
		ctx, cancel    = context.WithCancel(context.Background())
		pool           = NewWorkerPool(1)
		workersWait    = make(chan struct{})
		workersToStart sync.WaitGroup
		invocations    atomic.Int32
	)
	t.Cleanup(pool.Wait)

	worker := func(ctx context.Context, _ string) error {
		select {
		case <-ctx.Done():
			msg := `
			A worker was called with a cancelled context.
			This means there is a race between the context cancellation and another worker returning.
			If the worker currently in flight sees the closed channel before the pool sees the cancellation, it will return,
			and _this_ worker has started executing. The race is between the context cancellation and returning the
			semaphore in the pool.`
			t.Error(msg)
			return nil
		default:
		}
		invocations.Inc()
		workersToStart.Done()
		<-workersWait
		return nil
	}

	// We expect only a single one of the workers below to run. The other should be starved for concurrency.
	workersToStart.Add(1)
	go pool.ForEachNotInFlight(ctx, tokens, worker)
	workersToStart.Wait()

	// Cancel the context _before_ unblocking the worker to return.
	cancel()
	close(workersWait)
	pool.Wait()

	assert.Equal(t, int32(1), invocations.Load(), "expected one invocation before the context cancellation and none after it")
	invocations.Store(0)

	// Running the same tokens again should give us two invocations.
	workersToStart.Add(2)
	require.NoError(t, pool.ForEachNotInFlight(context.Background(), tokens, worker))
	assert.Equal(t, int32(2), invocations.Load(), "not both workers were called; maybe inflight tokens are in an invalid state")
}
