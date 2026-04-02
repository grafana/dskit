package test

import "testing"

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
//	ct := test.NewRecoverableT(t)
//	defer ct.Recover()
//
//	require.Equal(ct, expected, actual) // panics on failure instead of Goexit
type RecoverableT struct {
	testing.TB
}

type testFailure struct{}

// NewRecoverableT creates a new RecoverableT wrapping the given testing.TB.
func NewRecoverableT(t testing.TB) *RecoverableT {
	return &RecoverableT{TB: t}
}

// FailNow panics with a sentinel value instead of calling runtime.Goexit.
func (p *RecoverableT) FailNow() {
	panic(testFailure{})
}

// Recover catches the panic from FailNow. It must be called as a deferred function.
// Any non-sentinel panic is re-raised.
func (p *RecoverableT) Recover() {
	if r := recover(); r != nil {
		if _, ok := r.(testFailure); !ok {
			panic(r)
		}
	}
}
