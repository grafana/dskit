package retry

import (
	"errors"
	"time"
)

// MarkRetryable marks err as worth retrying, so that Marked reports it as
// retryable. It returns nil when err is nil.
func MarkRetryable(err error) error {
	return mark(err, 0)
}

// MarkRetryableAfter marks err as worth retrying no sooner than after, which
// the executor treats as a lower bound on the next delay. It returns nil when
// err is nil.
func MarkRetryableAfter(err error, after time.Duration) error {
	return mark(err, after)
}

// Marked is the default Decider. It retries what MarkRetryable and
// MarkRetryableAfter marked. It gives up on everything else, so an unmarked
// error gets exactly one attempt.
//
// Marked is an ordinary function, so it doubles as the read side of the two
// marking helpers and is useful outside a retry loop. It finds the first mark
// errors.As finds, so on the ordinary chain of wrapped errors the outermost
// After wins. For an error holding several others, such as one built by
// errors.Join, which mark it finds is unspecified: decide an aggregate in your
// own Decider, or mark the aggregate itself.
func Marked(err error) Decision {
	var m *marker
	if errors.As(err, &m) {
		return Decision{Retryable: true, After: m.after}
	}
	return Decision{}
}

var _ Decider = Marked

type marker struct {
	err   error
	after time.Duration
}

func mark(err error, after time.Duration) error {
	if err == nil {
		return nil
	}
	return &marker{err: err, after: after}
}

func (m *marker) Error() string { return m.err.Error() }

func (m *marker) Unwrap() error { return m.err }
