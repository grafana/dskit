package retry_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/grafana/dskit/retry"
)

// The default decider retries only what was marked at the point of failure, so
// an unmarked error gets exactly one attempt.
func Example() {
	err := retry.Retry(context.Background(), func(context.Context) error {
		return errors.New("bad request")
	}, retry.WithOperation("example"))

	fmt.Println(err)
	// Output: bad request
}

func ExampleRetry() {
	attempts := 0
	err := retry.Retry(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return retry.MarkRetryable(errors.New("connection refused"))
		}
		return nil
	}, retry.WithBackoff(retry.Constant(time.Millisecond)), retry.WithMaxAttempts(5))

	fmt.Println(attempts, err)
	// Output: 3 <nil>
}

// A component that retries the same way every time binds its options once.
func ExampleNew() {
	r := retry.New(
		retry.WithBackoff(retry.Constant(time.Millisecond)),
		retry.WithMaxAttempts(3),
		retry.WithOperation("sync"),
	)

	err := r.Retry(context.Background(), func(context.Context) error {
		return retry.MarkRetryable(errors.New("unavailable"))
	})

	fmt.Println(errors.Is(err, retry.ErrExhausted))
	fmt.Println(err)
	// Output:
	// true
	// retry budget exhausted after 3 attempts: unavailable
}

// statusError stands in for an error type of your own carrying a status code.
type statusError struct{ code int }

func (e *statusError) Error() string { return fmt.Sprintf("status %d", e.code) }

func ExampleWithPredicate() {
	isTransient := func(err error) bool {
		var status *statusError
		return errors.As(err, &status) && status.code >= 500
	}

	attempts := 0
	err := retry.Retry(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return &statusError{code: 503}
		}
		return &statusError{code: 400}
	}, retry.WithPredicate(isTransient), retry.WithBackoff(retry.Constant(0)))

	fmt.Println(attempts, err)
	// Output: 3 status 400
}

// A Decider is the longer form. More customizable.
func ExampleWithDecider() {
	decide := func(err error) retry.Decision {
		var status *statusError
		if !errors.As(err, &status) {
			return retry.Decision{}
		}
		switch {
		case status.code == 429:
			return retry.Decision{Retryable: true, After: time.Millisecond}
		case status.code >= 500:
			return retry.Decision{Retryable: true}
		default:
			return retry.Decision{}
		}
	}

	attempts := 0
	err := retry.Retry(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return &statusError{code: 429}
		}
		return &statusError{code: 400}
	}, retry.WithDecider(decide), retry.WithBackoff(retry.Constant(0)))

	fmt.Println(attempts, err)
	// Output: 3 status 400
}

// Deciders compose with a plain if, so a call site that mixes its own marked
// errors with errors from a library it does not own needs no combinator.
func ExampleMarked() {
	decide := func(err error) retry.Decision {
		if d := retry.Marked(err); d.Retryable {
			return d
		}
		var status *statusError
		return retry.Decision{Retryable: errors.As(err, &status) && status.code >= 500}
	}

	fmt.Println(decide(retry.MarkRetryable(&statusError{code: 404})).Retryable) // the mark wins
	fmt.Println(decide(&statusError{code: 503}).Retryable)                      // the fallback decides
	fmt.Println(decide(&statusError{code: 404}).Retryable)
	// Output:
	// true
	// true
	// false
}
