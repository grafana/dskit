// Package retry runs an operation again until it succeeds, until the decider
// gives up on a failure, until the attempt budget is spent, or until the context
// is done.
//
// The package owns the mechanism: an executor, a source of delays, a convention
// for marking errors, and metrics. Deciding what is worth retrying is left to
// the caller. Two pieces are pluggable, and each has a working default:
//
//   - a Decider (WithDecider), or the predicate that is its short form
//     (WithPredicate), says which errors deserve another attempt. The default is
//     Marked, which retries only what MarkRetryable and MarkRetryableAfter
//     recorded at the point the error was produced, so an unmarked error gets
//     exactly one attempt.
//   - a Backoff (WithBackoff) supplies the delays. The default is jittered
//     exponential growth. A Backoff says how long to wait, never how long to
//     keep going: the budget is WithMaxAttempts'.
//
// A predicate you already have is enough to get started:
//
//	err := retry.Retry(ctx, client.Sync, retry.WithPredicate(isTransient))
//
// A component that retries the same way every time builds one Retrier and
// keeps it, so its options are written once rather than at every call site:
//
//	r := retry.New(retry.WithPredicate(isTransient), retry.WithMetrics(m), retry.WithOperation("sync"))
//	err := r.Retry(ctx, client.Sync)
//
// A *Retrier is safe for concurrent use. The metrics are the piece to share
// rather than rebuild, because Prometheus panics on duplicate registration:
// build one *Metrics per registry with NewMetrics and pass it to every Retrier
// reporting there. They answer how often retrying happens, whether it ends in
// success, and what the waiting costs; see Metrics.
//
// The examples in this package are runnable, and README.md carries the longer
// discussion: how to choose a decider, what each built-in Backoff is for, which
// questions the metrics answer, and how nesting two loops behaves.
package retry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log/level"
)

// ErrExhausted is wrapped into the error returned when the attempt budget runs
// out, so callers can tell a spent budget from a terminal failure with
// errors.Is.
var ErrExhausted = errors.New("retry budget exhausted")

// Retrier runs operations under one fixed set of options. It keeps no
// per-operation state, so one Retrier serves any number of concurrent calls.
type Retrier struct {
	options
}

// New binds opts once, so that the returned Retrier can run any number of
// operations under them.
func New(opts ...Option) *Retrier {
	r := &Retrier{options: defaultOptions()}
	for _, opt := range opts {
		opt(&r.options)
	}
	return r
}

// Retry is shorthand for New(opts...).Retry(ctx, fn), so it builds the options
// again on every call. Prefer New when the same options serve more than one.
func Retry(ctx context.Context, fn func(context.Context) error, opts ...Option) error {
	return New(opts...).Retry(ctx, fn)
}

// Retry calls fn until it succeeds or until retrying stops making sense, and
// returns the error from the last attempt.
//
// ctx is passed to fn unchanged and bounds the whole loop, waits included. A
// context that is already done gets no attempt at all. Between two attempts the
// loop waits max(the Backoff's delay, the Decision's After).
//
// Retry returns:
//
//   - nil, once fn returns nil.
//   - an error wrapping ErrExhausted and the last failure, when the
//     WithMaxAttempts budget is spent.
//   - an error wrapping context.DeadlineExceeded and the last failure, as soon
//     as the next delay would outlast ctx's deadline. The wait is not served
//     out, because no attempt could follow it.
//   - an error wrapping the context's cause, and the last failure if there was
//     one, when ctx is done.
//   - the error from fn unchanged, when the decider gives up on it.
//
// Every reason but the last comes back with the failure it stopped on, and both
// are reachable with errors.Is.
//
// The loop never marks or unmarks an error of its own accord, so a failure
// marked retryable still reports so to Marked once the loop has given up on it:
// how the loop ended is what the wrapped reason says, not the mark. A loop
// nesting inside another has to read that before asking for one more round.
func (r *Retrier) Retry(ctx context.Context, fn func(context.Context) error) error {
	startTime := r.metrics.startTime()

	if ctx.Err() != nil {
		return r.finish(startTime, outcomeCancelled, stopped(context.Cause(ctx), 0, nil))
	}

	for attempt := 1; ; attempt++ {
		r.metrics.observeAttempt(r.operation)

		err := fn(ctx)
		if err == nil {
			return r.finish(startTime, success(attempt), nil)
		}

		// fn may report the cancellation as its own failure, so the cause is
		// the more useful of the two answers.
		if ctx.Err() != nil {
			return r.finish(startTime, outcomeCancelled, stopped(context.Cause(ctx), attempt, err))
		}

		decision := r.decider(err)
		if !decision.Retryable {
			return r.finish(startTime, outcomeTerminal, err)
		}

		if r.maxAttempts > 0 && attempt >= r.maxAttempts {
			return r.finish(startTime, outcomeExhausted, stopped(ErrExhausted, attempt, err))
		}

		delay := max(r.backoff(attempt), decision.After)
		if remaining, bounded := timeLeft(ctx); bounded && remaining <= delay {
			return r.finish(startTime, outcomeDeadline, stopped(context.DeadlineExceeded, attempt, err))
		}

		r.logRetry(attempt, delay, err)
		if !wait(ctx, delay) {
			return r.finish(startTime, outcomeCancelled, stopped(context.Cause(ctx), attempt, err))
		}
	}
}

// finish records how the loop ended and how long it took, and returns err, so
// that every exit from Retry reports an outcome.
func (r *Retrier) finish(startTime time.Time, outcome string, err error) error {
	r.metrics.observeOutcome(r.operation, outcome, startTime)
	return err
}

func (r *Retrier) logRetry(attempt int, delay time.Duration, err error) {
	if r.logger == nil {
		return
	}
	level.Debug(r.logger).Log(
		"msg", "operation failed, retrying",
		"operation", r.operation,
		"attempt", attempt,
		"delay", delay,
		"err", err,
	)
}

func success(attempts int) string {
	if attempts > 1 {
		return outcomeAfterRetry
	}
	return outcomeFirstAttempt
}

// stopped builds the error Retry returns when fn never succeeded: the reason
// the loop gave up, wrapped alongside the last failure so that errors.Is finds
// either one.
func stopped(reason error, attempts int, lastErr error) error {
	if lastErr == nil {
		return fmt.Errorf("%w after %d attempts", reason, attempts)
	}
	return fmt.Errorf("%w after %d attempts: %w", reason, attempts, lastErr)
}

// wait sleeps for delay, and reports whether it ran to completion rather than
// being cut short by ctx.
func wait(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// timeLeft reports how long ctx has to run, and whether it is bounded at all.
func timeLeft(ctx context.Context) (remaining time.Duration, bounded bool) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return time.Until(deadline), true
}
