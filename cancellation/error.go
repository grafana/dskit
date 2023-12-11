package cancellation

import (
	"context"
	"fmt"
)

type cancellationError struct {
	inner error
}

func NewError(err error) error {
	return cancellationError{err}
}

func NewErrorf(format string, args ...any) error {
	return NewError(fmt.Errorf(format, args...))
}

func (e cancellationError) Error() string {
	return "context canceled: " + e.inner.Error()
}

func (e cancellationError) Is(err error) bool {
	return err == context.Canceled
}

func (e cancellationError) Unwrap() error {
	return e.inner
}
