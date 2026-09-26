package retry

import (
	"time"

	"github.com/go-kit/log"
)

// Option configures a Retrier. Options are applied in order, so a later one
// overrides an earlier one, and passing nil to an option restores its default.
type Option func(*options)

// WithBackoff sets the delays between attempts. The default is a
// Jittered Exponential, growing from 100ms to 10s with half of each delay
// available to jitter.
//
// A Backoff says how long to wait, never whether to keep going: the budget is
// WithMaxAttempts'.
func WithBackoff(b Backoff) Option {
	return func(o *options) {
		if b == nil {
			b = defaultBackoff
		}
		o.backoff = b
	}
}

// WithDecider sets the decider that says which errors are retried, and may
// raise the delay before the next attempt. The default retries only errors
// marked with MarkRetryable or MarkRetryableAfter.
func WithDecider(d Decider) Option {
	return func(o *options) {
		if d == nil {
			d = Marked
		}
		o.decider = d
	}
}

// WithPredicate sets a predicate that says which errors are retried, leaving
// the delay to the Backoff. It is the short form of WithDecider, and a
// func(error) bool you already have goes straight in.
func WithPredicate(f func(error) bool) Option {
	if f == nil {
		return WithDecider(nil)
	}
	return WithDecider(func(err error) Decision {
		return Decision{Retryable: f(err)}
	})
}

// WithMetrics records attempts, outcomes and durations into m. A nil *Metrics
// records nothing, which is the default.
func WithMetrics(m *Metrics) Option {
	return func(o *options) { o.metrics = m }
}

// WithOperation names the operation being retried. It is the value of the
// operation label on every metric and appears in log lines.
func WithOperation(operation string) Option {
	return func(o *options) { o.operation = operation }
}

// WithMaxAttempts sets how many attempts an operation gets in total, including
// the first. The default is 10, and n below 1 is raised to 1, so no argument
// turns the budget off by accident; WithUnlimitedAttempts spells that out.
func WithMaxAttempts(n int) Option {
	return func(o *options) {
		if n < 1 {
			n = 1
		}
		o.maxAttempts = n
	}
}

// WithUnlimitedAttempts retries until the operation succeeds, the decider calls
// a failure terminal, or the context is done. Nothing else stops the loop, so
// pair it with a context that ends.
func WithUnlimitedAttempts() Option {
	return func(o *options) { o.maxAttempts = 0 }
}

// WithLogger logs a debug line before each retry, with the operation, the
// attempt number, the delay and the error being retried. A nil Logger logs
// nothing, which is the default.
func WithLogger(l log.Logger) Option {
	return func(o *options) { o.logger = l }
}

type options struct {
	backoff     Backoff
	decider     Decider
	metrics     *Metrics
	operation   string
	maxAttempts int
	logger      log.Logger
}

var defaultBackoff Backoff = Jittered(Exponential(100*time.Millisecond, 2, 10*time.Second), 0.5)

func defaultOptions() options {
	return options{
		backoff:     defaultBackoff,
		decider:     Marked,
		operation:   unknownOperation,
		maxAttempts: 10,
	}
}
