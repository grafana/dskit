package concurrency

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestLimitedConcurrencySingleFlight_ForEachNotInFlight_ConcurrencyLimit(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		sf             = NewLimitedConcurrencySingleFlight(10)
		workersWait    = make(chan struct{})
		workersToStart sync.WaitGroup
	)

	// Wait for the workers so we don't leak goroutines
	t.Cleanup(sf.Wait)
	t.Cleanup(func() { close(workersWait) })

	forEachNotInFlight := func(f func(context.Context, string) error, tokens ...string) {
		require.NoError(t, sf.ForEachNotInFlight(ctx, tokens, f))
	}

	busyWorker := func(ctx context.Context, s string) error {
		workersToStart.Done()
		<-workersWait
		return nil
	}

	workersToStart.Add(10)
	go forEachNotInFlight(busyWorker, "0", "1")
	go forEachNotInFlight(busyWorker, "2", "3")
	go forEachNotInFlight(busyWorker, "4", "5")
	go forEachNotInFlight(busyWorker, "6", "7")
	go forEachNotInFlight(busyWorker, "8", "9")
	workersToStart.Wait()

	extraWorkerInvoked := make(chan struct{})
	go forEachNotInFlight(func(ctx context.Context, s string) error {
		close(extraWorkerInvoked)
		return nil
	}, "10")

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
		sf             = NewLimitedConcurrencySingleFlight(10)
		workersWait    = make(chan struct{})
		workersToStart sync.WaitGroup
	)

	// Wait for the workers so we don't leak goroutines
	t.Cleanup(sf.Wait)
	t.Cleanup(func() { close(workersWait) })

	workersToStart.Add(1)
	go func() {
		require.NoError(t, sf.ForEachNotInFlight(ctx, []string{token}, func(ctx context.Context, s string) error {
			workersToStart.Done()
			<-workersWait
			return nil
		}))
	}()

	workersToStart.Wait()

	duplicatedTokenInvoked := false
	require.NoError(t, sf.ForEachNotInFlight(ctx, []string{token}, func(ctx context.Context, s string) error {
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
		sf             = NewLimitedConcurrencySingleFlight(10)
		workersWait    = make(chan struct{})
		workersToStart sync.WaitGroup
	)

	// Wait for the workers so we don't leak goroutines
	t.Cleanup(sf.Wait)
	t.Cleanup(func() { close(workersWait) })

	workersToStart.Add(1)
	go func() {
		require.NoError(t, sf.ForEachNotInFlight(ctx, []string{tokenA}, func(ctx context.Context, s string) error {
			workersToStart.Done()
			<-workersWait
			return nil
		}))
	}()

	workersToStart.Wait()
	var invocations atomic.Int64
	assert.NoError(t, sf.ForEachNotInFlight(ctx, []string{tokenA, tokenB}, func(ctx context.Context, s string) error {
		assert.Equal(t, tokenB, s)
		invocations.Inc()
		return nil
	}))

	assert.Equal(t, int64(1), invocations.Load(), "the same tokenA was invoked again while there is an in-flight invocation with it")
}

func TestLimitedConcurrencySingleFlight_ForEachNotInFlight_ReturnsWhenTokensAreEmpty(t *testing.T) {
	t.Parallel()

	var invocations atomic.Int64
	assert.NoError(t, NewLimitedConcurrencySingleFlight(10).ForEachNotInFlight(context.Background(), []string{}, func(ctx context.Context, s string) error {
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
		sf             = NewLimitedConcurrencySingleFlight(1)
		workersWait    = make(chan struct{})
		workersToStart sync.WaitGroup
		invocations    atomic.Int32
	)
	t.Cleanup(sf.Wait)

	worker := func(ctx context.Context, _ string) error {
		select {
		case <-ctx.Done():
			msg := `
			A worker was called with a cancelled context.
			This means there is a race between the context cancellation and another worker returning.
			If the worker currently in flight sees the closed channel before the singleFlight sees the cancellation, it will return,
			and _this_ worker has started executing. The race is between the context cancellation and returning the semaphore.`
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
	go func() { require.NoError(t, sf.ForEachNotInFlight(ctx, tokens, worker)) }()
	workersToStart.Wait()

	// Cancel the context _before_ unblocking the worker to return.
	cancel()
	close(workersWait)
	sf.Wait()

	assert.Equal(t, int32(1), invocations.Load(), "expected one invocation before the context cancellation and none after it")
	invocations.Store(0)

	// Running the same tokens again should give us two invocations.
	workersToStart.Add(2)
	require.NoError(t, sf.ForEachNotInFlight(context.Background(), tokens, worker))
	assert.Equal(t, int32(2), invocations.Load(), "not both workers were called; maybe inflight tokens are in an invalid state")
}
