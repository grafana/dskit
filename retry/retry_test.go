package retry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

// failFor returns a function failing with err for the first n attempts, and a
// pointer to the number of times it was called.
func failFor(n int, err error) (func(context.Context) error, *int) {
	calls := 0
	return func(context.Context) error {
		calls++
		if calls <= n {
			return err
		}
		return nil
	}, &calls
}

// alwaysFails returns a function failing with err however many times it is
// called, and a pointer to the number of times it was. A loop only the context
// ends has to outlast whatever the loop can count to.
func alwaysFails(err error) (func(context.Context) error, *int) {
	calls := 0
	return func(context.Context) error {
		calls++
		return err
	}, &calls
}

func TestRetry_succeedsOnFirstAttempt(t *testing.T) {
	fn, calls := failFor(0, errBoom)

	require.NoError(t, Retry(context.Background(), fn, WithBackoff(Constant(0)), WithMaxAttempts(3)))
	assert.Equal(t, 1, *calls)
}

func TestRetry_retriesUntilSuccess(t *testing.T) {
	fn, calls := failFor(2, MarkRetryable(errBoom))

	require.NoError(t, Retry(context.Background(), fn, WithBackoff(Constant(0)), WithMaxAttempts(5)))
	assert.Equal(t, 3, *calls)
}

func TestRetry_anErrorTheDeciderGivesUpOnIsReturnedUnchanged(t *testing.T) {
	fn, calls := failFor(5, errBoom)

	err := Retry(context.Background(), fn, WithBackoff(Constant(0)), WithMaxAttempts(5))
	assert.Same(t, errBoom, err, "nothing is added to an error this loop did not retry")
	assert.Equal(t, 1, *calls, "an unmarked error gets exactly one attempt")
}

func TestRetry_predicateDecidesWhatIsRetried(t *testing.T) {
	t.Run("retries what it accepts", func(t *testing.T) {
		fn, calls := failFor(2, &statusError{code: 503})

		require.NoError(t, Retry(context.Background(), fn,
			WithBackoff(Constant(0)), WithMaxAttempts(5),
			WithPredicate(isRetryableStatus),
		))
		assert.Equal(t, 3, *calls, "an unmarked error is retried when the predicate says so")
	})

	t.Run("gives up on what it rejects", func(t *testing.T) {
		fn, calls := failFor(5, &statusError{code: 404})

		err := Retry(context.Background(), fn,
			WithBackoff(Constant(0)), WithMaxAttempts(5),
			WithPredicate(isRetryableStatus),
		)
		assert.EqualError(t, err, "status 404")
		assert.Equal(t, 1, *calls)
	})
}

func TestRetry_exhaustionReportsTheBudgetAndTheLastFailure(t *testing.T) {
	fn, calls := failFor(10, MarkRetryable(errBoom))

	err := Retry(context.Background(), fn, WithBackoff(Constant(0)), WithMaxAttempts(3))
	require.Error(t, err)
	assert.Equal(t, 3, *calls)

	assert.ErrorIs(t, err, ErrExhausted)
	assert.ErrorIs(t, err, errBoom)
}

func TestRetry_deciderSeesTheError(t *testing.T) {
	var seen []error
	fn, _ := failFor(1, errBoom)

	require.NoError(t, Retry(context.Background(), fn,
		WithBackoff(Constant(0)), WithMaxAttempts(3),
		WithDecider(func(err error) Decision {
			seen = append(seen, err)
			return Decision{Retryable: true}
		}),
	))
	assert.Equal(t, []error{errBoom}, seen)
}

func TestRetry_serverFloorRaisesTheDelay(t *testing.T) {
	start := time.Now()
	fn, calls := failFor(1, MarkRetryableAfter(errBoom, 20*time.Millisecond))

	require.NoError(t, Retry(context.Background(), fn, WithBackoff(Constant(0)), WithMaxAttempts(3)))
	assert.Equal(t, 2, *calls)
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond,
		"After is a lower bound on the delay, even when the backoff asks for less")
}

func TestRetry_contextDoneBeforeFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fn, calls := failFor(0, errBoom)

	assert.ErrorIs(t, Retry(ctx, fn, WithBackoff(Constant(0)), WithMaxAttempts(3)), context.Canceled)
	assert.Zero(t, *calls)
}

func TestRetry_contextDoneDuringAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn, calls := failFor(10, MarkRetryable(errBoom))
	err := Retry(ctx, func(c context.Context) error {
		cancel()
		return fn(c)
	}, WithBackoff(Constant(time.Hour)), WithMaxAttempts(5))

	require.Error(t, err)
	assert.Equal(t, 1, *calls)
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, err, errBoom, "the last failure is kept alongside the cancellation")
}

func TestRetry_contextDoneWhileWaiting(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	// No deadline to clip the delay against, so the loop commits to the wait
	// and is interrupted in it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	fn, calls := failFor(10, MarkRetryable(errBoom))
	start := time.Now()
	err := Retry(ctx, fn, WithBackoff(Constant(time.Hour)), WithMaxAttempts(5), WithMetrics(NewMetrics(reg)))

	require.Error(t, err)
	assert.Equal(t, 1, *calls)
	assert.Less(t, time.Since(start), time.Minute, "the wait is interrupted rather than slept through")
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, err, errBoom)
	assert.Equal(t, uint64(1), outcomeCount(t, reg, "unknown", outcomeCancelled))
}

func TestRetry_doesNotWaitPastTheDeadline(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	fn, calls := failFor(10, MarkRetryable(errBoom))
	start := time.Now()
	err := Retry(ctx, fn,
		WithBackoff(Constant(time.Hour)),
		WithMaxAttempts(5),
		WithMetrics(NewMetrics(reg)),
		WithOperation("sync"),
	)
	elapsed := time.Since(start)

	assert.Equal(t, 1, *calls)
	assert.Less(t, elapsed, time.Second, "a delay that outlasts the deadline is not served out")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorIs(t, err, errBoom)

	assert.Equal(t, uint64(1), outcomeCount(t, reg, "sync", outcomeDeadline),
		"giving up early is not the same outcome as an ended context")
	assert.Zero(t, outcomeCount(t, reg, "sync", outcomeCancelled))
}

func TestRetry_waitsWhenTheDelayFitsInTheDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	fn, calls := failFor(2, MarkRetryable(errBoom))
	require.NoError(t, Retry(ctx, fn, WithBackoff(Constant(time.Millisecond)), WithMaxAttempts(5)))
	assert.Equal(t, 3, *calls)
}

func TestRetry_deadlineClipRespectsTheServerFloor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// The backoff fits, but the server asked for longer than the deadline.
	fn, calls := failFor(10, MarkRetryableAfter(errBoom, time.Hour))
	start := time.Now()
	err := Retry(ctx, fn, WithBackoff(Constant(time.Millisecond)), WithMaxAttempts(5))

	assert.Equal(t, 1, *calls)
	assert.Less(t, time.Since(start), time.Second)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRetry_contextCauseIsReported(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errBoom)

	assert.ErrorIs(t, Retry(ctx, func(context.Context) error { return nil }), errBoom)
}

func TestRetry_contextIsPassedThroughUnchanged(t *testing.T) {
	ctx := context.WithValue(context.Background(), testKey{}, "value")

	require.NoError(t, Retry(ctx, func(c context.Context) error {
		assert.Equal(t, "value", c.Value(testKey{}))
		_, hasDeadline := c.Deadline()
		assert.False(t, hasDeadline)
		return nil
	}))
}

type testKey struct{}

func TestRetry_optionsAreAppliedInOrder(t *testing.T) {
	fn, calls := failFor(10, MarkRetryable(errBoom))

	err := Retry(context.Background(), fn, WithMaxAttempts(9), WithMaxAttempts(2), WithBackoff(Constant(0)))
	assert.ErrorIs(t, err, ErrExhausted)
	assert.Equal(t, 2, *calls)
}

func TestRetry_nilOptionsRestoreTheDefaults(t *testing.T) {
	t.Run("backoff", func(t *testing.T) {
		fn, calls := failFor(1, MarkRetryable(errBoom))

		require.NoError(t, Retry(context.Background(), fn, WithBackoff(nil), WithMaxAttempts(2)))
		assert.Equal(t, 2, *calls, "a nil backoff still retries, and still waits")
	})

	t.Run("decider", func(t *testing.T) {
		fn, calls := failFor(1, MarkRetryable(errBoom))

		require.NoError(t, Retry(context.Background(), fn,
			WithBackoff(Constant(0)), WithMaxAttempts(3), WithDecider(nil)))
		assert.Equal(t, 2, *calls, "a nil decider still reads the marks")
	})

	t.Run("predicate", func(t *testing.T) {
		fn, calls := failFor(1, MarkRetryable(errBoom))

		require.NoError(t, Retry(context.Background(), fn,
			WithBackoff(Constant(0)), WithMaxAttempts(3), WithPredicate(nil)))
		assert.Equal(t, 2, *calls, "a nil predicate still reads the marks")
	})

	t.Run("logger", func(t *testing.T) {
		fn, calls := failFor(1, MarkRetryable(errBoom))

		require.NoError(t, Retry(context.Background(), fn,
			WithBackoff(Constant(0)), WithMaxAttempts(3), WithLogger(nil)))
		assert.Equal(t, 2, *calls)
	})
}

func TestRetry_logsEachRetry(t *testing.T) {
	var buf bytes.Buffer
	fn, _ := failFor(1, MarkRetryable(errBoom))

	require.NoError(t, Retry(context.Background(), fn,
		WithBackoff(Constant(0)), WithMaxAttempts(3),
		WithOperation("sync"),
		WithLogger(log.NewLogfmtLogger(&buf)),
	))

	line := buf.String()
	assert.Equal(t, 1, strings.Count(line, "\n"))
	assert.Contains(t, line, "operation=sync")
	assert.Contains(t, line, "attempt=1")
	assert.Contains(t, line, "err=boom")
}

func TestRetry_isSafeForConcurrentUseOfOneRetrier(t *testing.T) {
	r := New(WithBackoff(Constant(0)), WithMaxAttempts(5))

	done := make(chan error, 4)
	for range cap(done) {
		go func() {
			fn, _ := failFor(2, MarkRetryable(errBoom))
			done <- r.Retry(context.Background(), fn)
		}()
	}
	for range cap(done) {
		require.NoError(t, <-done)
	}
}

func TestRetry_maxAttemptsDefaultsToTen(t *testing.T) {
	fn, calls := failFor(100, MarkRetryable(errBoom))

	err := Retry(context.Background(), fn, WithBackoff(Constant(0)))
	assert.ErrorIs(t, err, ErrExhausted)
	assert.Equal(t, 10, *calls)
}

func TestRetry_maxAttemptsBelowOneIsASingleAttempt(t *testing.T) {
	for _, n := range []int{1, 0, -1} {
		fn, calls := failFor(10, MarkRetryable(errBoom))

		err := Retry(context.Background(), fn, WithBackoff(Constant(0)), WithMaxAttempts(n))
		assert.ErrorIs(t, err, ErrExhausted, "WithMaxAttempts(%d)", n)
		assert.Equal(t, 1, *calls, "WithMaxAttempts(%d) must not turn the budget off", n)
	}
}

func TestRetry_unlimitedAttemptsRunsUntilTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	fn, calls := alwaysFails(MarkRetryable(errBoom))
	err := Retry(ctx, fn, WithBackoff(Constant(0)), WithUnlimitedAttempts())

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotErrorIs(t, err, ErrExhausted)
	assert.Greater(t, *calls, 10, "the default budget must not apply")
}

func TestRetrier_isReusable(t *testing.T) {
	r := New(WithBackoff(Constant(0)), WithMaxAttempts(3))

	for range 3 {
		fn, calls := failFor(10, MarkRetryable(errBoom))
		assert.ErrorIs(t, r.Retry(context.Background(), fn), ErrExhausted)
		assert.Equal(t, 3, *calls, "every call gets the same budget")
	}
}

func TestRetry_isNewPlusRetry(t *testing.T) {
	opts := []Option{WithBackoff(Constant(0)), WithMaxAttempts(4)}

	fn, calls := failFor(10, MarkRetryable(errBoom))
	viaFunc := Retry(context.Background(), fn, opts...)

	fn2, calls2 := failFor(10, MarkRetryable(errBoom))
	viaRetrier := New(opts...).Retry(context.Background(), fn2)

	assert.Equal(t, *calls, *calls2)
	assert.Equal(t, viaFunc.Error(), viaRetrier.Error())
}

func TestRetry_errorMessages(t *testing.T) {
	fn, _ := failFor(10, MarkRetryable(errBoom))
	exhausted := Retry(context.Background(), fn, WithBackoff(Constant(0)), WithMaxAttempts(2))
	assert.EqualError(t, exhausted, "retry budget exhausted after 2 attempts: boom")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.EqualError(t, Retry(ctx, func(context.Context) error { return nil }),
		"context canceled after 0 attempts")

	deadlined, cancelDeadline := context.WithTimeout(context.Background(), time.Minute)
	defer cancelDeadline()
	fn2, _ := failFor(10, MarkRetryable(errBoom))
	assert.EqualError(t, Retry(deadlined, fn2, WithBackoff(Constant(time.Hour))),
		"context deadline exceeded after 1 attempts: boom")
}

// TestRetry_leavesTheMarksOnTheLastFailure pins what a nesting loop works
// against: how this one stopped is what the wrapped reason says, not the mark.
func TestRetry_leavesTheMarksOnTheLastFailure(t *testing.T) {
	// Buried under a caller's own wrapping, where a mark usually is by the time
	// an outer loop sees it.
	failure := fmt.Errorf("writing: %w", MarkRetryable(errBoom))

	t.Run("exhausted", func(t *testing.T) {
		fn, _ := failFor(10, failure)
		err := Retry(context.Background(), fn, WithBackoff(Constant(0)), WithMaxAttempts(2))

		assert.Equal(t, Decision{Retryable: true}, Marked(err), "the mark is the caller's, and stays")
		assert.ErrorIs(t, err, ErrExhausted, "a spent budget is what a nested loop checks for")
		assert.ErrorIs(t, err, errBoom, "the last failure stays reachable")
	})

	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		fn, _ := failFor(10, failure)
		err := Retry(ctx, fn, WithBackoff(Constant(time.Hour)), WithMaxAttempts(5))

		assert.Equal(t, Decision{Retryable: true}, Marked(err))
		assert.ErrorIs(t, err, context.Canceled, "the ended context is what a nested loop checks for")
		assert.ErrorIs(t, err, errBoom, "the last failure stays reachable")
	})

	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		fn, _ := failFor(10, failure)
		err := Retry(ctx, fn, WithBackoff(Constant(time.Hour)))

		assert.Equal(t, Decision{Retryable: true}, Marked(err))
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.ErrorIs(t, err, errBoom)
	})
}
