package retry

import "time"

// Decision is what a Decider reports about one failed attempt.
type Decision struct {
	// Retryable says whether another attempt has a chance of succeeding. The
	// zero Decision gives up, so a decider that does not recognize an error
	// fails closed by default.
	Retryable bool

	// After, when non-zero, is a lower bound on the delay before the next
	// attempt, such as a server-supplied Retry-After. The executor waits
	// max(backoff delay, After).
	After time.Duration
}

// Decider decides whether an error is worth retrying, and may raise the delay
// before the next attempt.
//
// The executor never calls a Decider with a nil error, so implementations need
// no nil check.
type Decider func(err error) Decision
