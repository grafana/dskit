package test

import (
	"reflect"
	"testing"
	"time"
)

// Eventually repeatedly calls the fn until it returns true or until the duration is exceeded.
// The fn will be called up to 100 times.
func Eventually(t testing.TB, duration time.Duration, fn func() bool) {
	EventuallyEqual[bool](t, duration, true, fn)
}

// EventuallyEqual repeatedly calls the fn until its return value equals expected or until the duration is exceeded.
// The fn will be called up to 100 times.
func EventuallyEqual[T any](t testing.TB, duration time.Duration, expected T, fn func() T) {
	Poll(t, duration, expected, func() interface{} {
		return fn()
	})
}

// Poll repeatedly calls have until its return value matches expected or until the duration is exceeded.
// The fn will be called up to 100 times.
func Poll(t testing.TB, duration time.Duration, expected interface{}, have func() interface{}) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for {
		if time.Now().After(deadline) {
			break
		}
		if reflect.DeepEqual(expected, have()) {
			return
		}
		time.Sleep(duration / 100)
	}
	h := have()
	if !reflect.DeepEqual(expected, h) {
		t.Fatalf("expected %v, got %v", expected, h)
	}
}
