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
	var workersInvoked atomic.Int64
	assert.NoError(t, pool.ForEachNotInFlight(ctx, []string{tokenA, tokenB}, func(ctx context.Context, s string) error {
		assert.Equal(t, tokenB, s)
		workersInvoked.Inc()
		return nil
	}))

	assert.Equal(t, int64(1), workersInvoked.Load(), "the same tokenA was invoked again while there is an in-flight invocation with it")
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
