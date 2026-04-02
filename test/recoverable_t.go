package test

import (
	"errors"
	"testing"
)

// RecoverableT wraps a testing.TB so that FailNow panics instead of calling
// runtime.Goexit. This allows using require.* assertions from non-test goroutines
// (e.g., workers in concurrency.ForEachJob) where runtime.Goexit would prevent the
// goroutine from returning and cause the caller to hang.
//
// Errorf and all other methods delegate to the wrapped testing.TB, so test failures
// are still recorded normally.
//
// Usage:
//
//	t := test.NewRecoverableT(t)
//	defer t.Recover()
//
//	require.Equal(t, expected, actual) // panics on failure instead of Goexit
type RecoverableT struct {
	testing.TB
}

type testFailure struct{}

// ErrTestFailed is returned by Recover when a test assertion failure was caught.
var ErrTestFailed = errors.New("test assertion failed")

// NewRecoverableT creates a new RecoverableT wrapping the given testing.TB.
func NewRecoverableT(t testing.TB) *RecoverableT {
	return &RecoverableT{TB: t}
}

// FailNow panics with a sentinel value instead of calling runtime.Goexit.
func (p *RecoverableT) FailNow() {
	panic(testFailure{})
}

// Recover catches the panic from FailNow. It must be called as a deferred function.
// Any non-sentinel panic is re-raised. When a test failure is caught, the enclosing
// function returns its zero values (e.g., nil error). Use RecoverError in functions
// that should return ErrTestFailed to signal the failure to the caller.
func (p *RecoverableT) Recover() {
	if r := recover(); r != nil {
		if _, ok := r.(testFailure); !ok {
			panic(r)
		}
	}
}

// RecoverError catches the panic from FailNow and assigns ErrTestFailed to the
// provided error pointer. It must be called as a deferred function with a pointer
// to a named return value. Any non-sentinel panic is re-raised.
//
// Usage:
//
//	func doWork() (retErr error) {
//	    t := test.NewRecoverableT(t)
//	    defer t.RecoverError(&retErr)
//	    require.Equal(t, expected, actual)
//	    return nil
//	}
func (p *RecoverableT) RecoverError(errPtr *error) {
	if r := recover(); r != nil {
		if _, ok := r.(testFailure); !ok {
			panic(r)
		}
		*errPtr = ErrTestFailed
	}
}
