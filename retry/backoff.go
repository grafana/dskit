package retry

import (
	"math"
	"math/rand/v2"
	"time"
)

// maxDuration is the largest delay a time.Duration can hold. Converting a
// float64 above it to an integer is undefined in Go, and yields a negative
// duration on some platforms, which would turn a wait into a hot loop.
const maxDuration = float64(math.MaxInt64)

// Backoff reports how long to wait after attempts failed attempts. attempts
// is the number of attempts made so far, so it is 1 after the first failure.
//
// A Backoff is a pure function of the attempt count. It holds no state, so
// one Backoff is safe to share across goroutines and across calls, and it
// never ends a sequence: the budget is WithMaxAttempts', the waiting and the
// cancellation are the executor's.
type Backoff func(attempts int) time.Duration

// Exponential returns a Backoff that waits start after the first failure and
// multiplies the wait by factor after each one that follows, never exceeding
// maxDelay. Wrap it in Jittered so that callers that failed together do not
// retry together.
//
// Arguments outside their usable range are clamped: a factor below one, or one
// that is not a number, to one. A maxDelay of zero or less leaves the growth
// unbounded, up to what a time.Duration can hold; a start of zero or less waits
// not at all, whatever the factor.
func Exponential(start time.Duration, factor float64, maxDelay time.Duration) Backoff {
	if start <= 0 {
		return Constant(0)
	}
	if math.IsNaN(factor) || factor < 1 {
		factor = 1
	}

	return func(attempts int) time.Duration {
		delay := float64(start) * math.Pow(factor, float64(attempts-1))
		if maxDelay > 0 && delay > float64(maxDelay) {
			return maxDelay
		}
		if delay >= maxDuration {
			return math.MaxInt64
		}
		return time.Duration(delay)
	}
}

// Linear returns a Backoff that waits start after the first failure and adds
// factor of start after each one that follows, never exceeding maxDelay. It
// grows by a constant increment where Exponential grows by a constant ratio.
// Wrap it in Jittered so that callers that failed together do not retry
// together.
//
// A factor of 1 gives start, 2*start, 3*start and so on; a factor of 0 never
// grows, which is Constant. Arguments outside their usable range are clamped: a
// factor below zero, or one that is not a number, to zero. A maxDelay of zero or
// less leaves the growth unbounded, up to what a time.Duration can hold; a start
// of zero or less waits not at all, whatever the factor.
func Linear(start time.Duration, factor float64, maxDelay time.Duration) Backoff {
	if start <= 0 {
		return Constant(0)
	}
	if math.IsNaN(factor) || factor < 0 {
		factor = 0
	}

	return func(attempts int) time.Duration {
		// The increment is added only once there is something to add it to. An
		// attempt count below the documented minimum would otherwise subtract
		// from start, and an infinite factor times a zero count is not a number.
		delay := float64(start)
		if attempts > 1 {
			delay += float64(start) * factor * float64(attempts-1)
		}
		if maxDelay > 0 && delay > float64(maxDelay) {
			return maxDelay
		}
		if delay >= maxDuration {
			return math.MaxInt64
		}
		return time.Duration(delay)
	}
}

// Uniform returns a Backoff that waits a duration drawn uniformly from
// [minDelay, maxDelay) after every failed attempt, ignoring how many there have
// been.
//
// A minDelay below zero is raised to zero, and a maxDelay at or below minDelay
// waits exactly minDelay.
func Uniform(minDelay, maxDelay time.Duration) Backoff {
	if minDelay < 0 {
		minDelay = 0
	}
	if maxDelay <= minDelay {
		return Constant(minDelay)
	}
	spread := int64(maxDelay - minDelay)

	return func(int) time.Duration {
		return minDelay + time.Duration(rand.Int64N(spread))
	}
}

// Constant returns a Backoff that waits d after every failed attempt.
func Constant(d time.Duration) Backoff {
	return func(int) time.Duration { return d }
}

// Jittered returns a Backoff that shortens each of b's delays by up to
// fraction of itself, drawn uniformly, so that callers that failed together do
// not retry together.
//
// fraction is a share of the delay, clamped to [0, 1]: 0 returns b untouched,
// as does a fraction that is not a number; 0.5 shortens each delay by up to a
// half, and 1 draws from the whole interval below it. Jitter only ever
// subtracts, so whatever ceiling b has is a true ceiling on the wait.
func Jittered(b Backoff, fraction float64) Backoff {
	if math.IsNaN(fraction) || fraction <= 0 {
		return b
	}
	if fraction > 1 {
		fraction = 1
	}

	return func(attempts int) time.Duration {
		delay := b(attempts)
		if delay <= 0 {
			return delay
		}
		return delay - time.Duration(float64(delay)*fraction*rand.Float64())
	}
}
