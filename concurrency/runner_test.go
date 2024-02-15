package concurrency

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/goleak"
)

func TestForEachUser(t *testing.T) {
	var (
		// Keep track of processed users.
		processedMx sync.Mutex
		processed   []string
	)

	input := []string{"a", "b", "c"}

	err := ForEachUser(context.Background(), input, 2, func(ctx context.Context, user string) error {
		processedMx.Lock()
		defer processedMx.Unlock()
		processed = append(processed, user)
		return nil
	})

	require.NoError(t, err)
	assert.ElementsMatch(t, input, processed)
}

func TestForEachUser_ShouldContinueOnErrorButReturnIt(t *testing.T) {
	// Keep the processed users count.
	var processed atomic.Int32

	input := []string{"a", "b", "c"}

	err := ForEachUser(context.Background(), input, 2, func(ctx context.Context, user string) error {
		if processed.CompareAndSwap(0, 1) {
			return errors.New("the first request is failing")
		}

		// Wait 1s and increase the number of processed jobs, unless the context get canceled earlier.
		select {
		case <-time.After(time.Second):
			processed.Add(1)
		case <-ctx.Done():
			return ctx.Err()
		}

		return nil
	})

	require.EqualError(t, err, "the first request is failing")

	// Since we expect it continues on error, the number of processed users should be equal to the input length.
	assert.Equal(t, int32(len(input)), processed.Load())
}

func TestForEachUser_ShouldReturnImmediatelyOnNoUsersProvided(t *testing.T) {
	require.NoError(t, ForEachUser(context.Background(), nil, 2, func(ctx context.Context, user string) error {
		return nil
	}))
}

func TestForEachJob(t *testing.T) {
	jobs := []string{"a", "b", "c"}
	processed := make([]string, len(jobs))

	err := ForEachJob(context.Background(), len(jobs), 2, func(ctx context.Context, idx int) error {
		processed[idx] = jobs[idx]
		return nil
	})

	require.NoError(t, err)
	assert.ElementsMatch(t, jobs, processed)
}

func TestForEachJob_ShouldBreakOnFirstError_ContextCancellationHandled(t *testing.T) {
	// Keep the processed jobs count.
	var processed atomic.Int32

	err := ForEachJob(context.Background(), 3, 2, func(ctx context.Context, idx int) error {
		if processed.CompareAndSwap(0, 1) {
			return errors.New("the first request is failing")
		}

		// Wait 1s and increase the number of processed jobs, unless the context get canceled earlier.
		select {
		case <-time.After(time.Second):
			processed.Add(1)
		case <-ctx.Done():
			return ctx.Err()
		}

		return nil
	})

	require.EqualError(t, err, "the first request is failing")

	// Since we expect the first error interrupts the workers, we should only see
	// 1 job processed (the one which immediately returned error).
	assert.Equal(t, int32(1), processed.Load())
}

func TestForEachJob_ShouldBreakOnFirstError_ContextCancellationUnhandled(t *testing.T) {
	// Keep the processed jobs count.
	var processed atomic.Int32

	// waitGroup to await the start of the first two jobs
	var wg sync.WaitGroup
	wg.Add(2)

	err := ForEachJob(context.Background(), 3, 2, func(ctx context.Context, idx int) error {
		wg.Done()

		if processed.CompareAndSwap(0, 1) {
			// wait till two jobs have been started
			wg.Wait()
			return errors.New("the first request is failing")
		}

		// Wait till context is cancelled to add processed jobs.
		<-ctx.Done()
		processed.Add(1)

		return nil
	})

	require.EqualError(t, err, "the first request is failing")

	// Since we expect the first error interrupts the workers, we should only
	// see 2 job processed (the one which immediately returned error and the
	// job with "b").
	assert.Equal(t, int32(2), processed.Load())
}

func TestForEachJob_ShouldReturnImmediatelyOnNoJobsProvided(t *testing.T) {
	// Keep the processed jobs count.
	var processed atomic.Int32
	require.NoError(t, ForEachJob(context.Background(), 0, 2, func(ctx context.Context, idx int) error {
		processed.Inc()
		return nil
	}))
	require.Zero(t, processed.Load())
}

func TestForEach(t *testing.T) {
	var (
		// Keep track of processed jobs.
		processedMx sync.Mutex
		processed   []string
	)

	jobs := []string{"a", "b", "c"}

	err := ForEach(context.Background(), CreateJobsFromStrings(jobs), 2, func(ctx context.Context, job interface{}) error {
		processedMx.Lock()
		defer processedMx.Unlock()
		processed = append(processed, job.(string))
		return nil
	})

	require.NoError(t, err)
	assert.ElementsMatch(t, jobs, processed)
}

func TestForEach_ShouldBreakOnFirstError_ContextCancellationHandled(t *testing.T) {
	var (
		ctx = context.Background()

		// Keep the processed jobs count.
		processed atomic.Int32
	)

	err := ForEach(ctx, []interface{}{"a", "b", "c"}, 2, func(ctx context.Context, job interface{}) error {
		if processed.CompareAndSwap(0, 1) {
			return errors.New("the first request is failing")
		}

		// Wait 1s and increase the number of processed jobs, unless the context get canceled earlier.
		select {
		case <-time.After(time.Second):
			processed.Add(1)
		case <-ctx.Done():
			return ctx.Err()
		}

		return nil
	})

	require.EqualError(t, err, "the first request is failing")

	// Since we expect the first error interrupts the workers, we should only see
	// 1 job processed (the one which immediately returned error).
	assert.Equal(t, int32(1), processed.Load())
}

func TestForEach_ShouldBreakOnFirstError_ContextCancellationUnhandled(t *testing.T) {
	// Keep the processed jobs count.
	var processed atomic.Int32

	// waitGroup to await the start of the first two jobs
	var wg sync.WaitGroup
	wg.Add(2)

	err := ForEach(context.Background(), []interface{}{"a", "b", "c"}, 2, func(ctx context.Context, job interface{}) error {
		wg.Done()

		if processed.CompareAndSwap(0, 1) {
			// wait till two jobs have been started
			wg.Wait()
			return errors.New("the first request is failing")
		}

		// Wait till context is cancelled to add processed jobs.
		<-ctx.Done()
		processed.Add(1)

		return nil
	})

	require.EqualError(t, err, "the first request is failing")

	// Since we expect the first error interrupts the workers, we should only
	// see 2 job processed (the one which immediately returned error and the
	// job with "b").
	assert.Equal(t, int32(2), processed.Load())
}

func TestForEach_ShouldReturnImmediatelyOnNoJobsProvided(t *testing.T) {
	require.NoError(t, ForEach(context.Background(), nil, 2, func(ctx context.Context, job interface{}) error {
		return nil
	}))
}

func TestForEachJobMergeResults(t *testing.T) {
	// Ensure none of these tests leak goroutines.
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	generateCallbackFunction := func() func(context.Context, []string) ([]string, error) {
		return func(_ context.Context, job []string) ([]string, error) {
			return job, nil
		}
	}

	t.Run("should return no results and no error on no jobs", func(t *testing.T) {
		actual, err := ForEachJobMergeResults[[]string, string](context.Background(), nil, 0, generateCallbackFunction())
		require.NoError(t, err)
		assert.Empty(t, actual)
	})

	t.Run("should call the function once for each input job and merge the results", func(t *testing.T) {
		jobs := [][]string{
			{"1", "2"},
			{"3"},
			{"4", "5"},
		}

		actual, err := ForEachJobMergeResults[[]string, string](context.Background(), jobs, 0, generateCallbackFunction())
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"1", "2", "3", "4", "5"}, actual)
	})

	t.Run("should call the function concurrently for every input job", func(t *testing.T) {
		jobs := [][]string{
			{"1"},
			{"2"},
			{"3"},
			{"4"},
			{"5"},
		}

		startTime := time.Now()
		actual, err := ForEachJobMergeResults[[]string, string](context.Background(), jobs, 0, func(_ context.Context, job []string) ([]string, error) {
			time.Sleep(time.Second)
			return job, nil
		})

		require.Less(t, time.Since(startTime), 2*time.Second)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"1", "2", "3", "4", "5"}, actual)
	})

	t.Run("should return as soon as the first error is returned by the callback function", func(t *testing.T) {
		jobs := [][]string{
			{"1"},
			{"2"},
			{"3"},
		}

		wg := sync.WaitGroup{}
		wg.Add(len(jobs))

		startTime := time.Now()
		actual, err := ForEachJobMergeResults[[]string, string](context.Background(), jobs, 0, func(ctx context.Context, job []string) ([]string, error) {
			defer wg.Done()

			if len(job) == 1 && job[0] == "3" {
				return nil, errors.New("mocked error")
			}

			select {
			// We expect context to get canceled.
			case <-ctx.Done():

			// Slow down successful executions.
			case <-time.After(5 * time.Second):
				require.Fail(t, "context has not been canceled")
			}

			return job, nil
		})

		require.Less(t, time.Since(startTime), time.Second)
		require.Error(t, err)
		assert.Empty(t, actual)

		// Wait until all callback functions return.
		wg.Wait()
	})

	t.Run("should not leak goroutines when all callback functions return an error nearly at the same time", func(t *testing.T) {
		jobs := [][]string{
			{"1"},
			{"2"},
			{"3"},
		}

		waitBeforeReturningError := make(chan struct{})
		callbacksStarted := sync.WaitGroup{}
		callbacksStarted.Add(len(jobs))

		go func() {
			// Wait until all callback functions start.
			callbacksStarted.Wait()

			// Let all goroutines returning error.
			close(waitBeforeReturningError)
		}()

		_, err := ForEachJobMergeResults[[]string, string](context.Background(), jobs, 0, func(ctx context.Context, job []string) ([]string, error) {
			callbacksStarted.Done()
			<-waitBeforeReturningError

			return nil, errors.New("mocked error")
		})

		require.Error(t, err)
	})
}
