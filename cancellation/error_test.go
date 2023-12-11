package cancellation

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCancellationError(t *testing.T) {
	directlyNestedErr := NewError(io.ErrNoProgress)
	require.True(t, errors.Is(directlyNestedErr, context.Canceled))
	require.True(t, errors.Is(directlyNestedErr, io.ErrNoProgress))
	require.False(t, errors.Is(directlyNestedErr, io.EOF))

	indirectlyNestedErr := NewErrorf("something went wrong: %w", io.ErrNoProgress)
	require.True(t, errors.Is(indirectlyNestedErr, context.Canceled))
	require.True(t, errors.Is(indirectlyNestedErr, io.ErrNoProgress))
	require.False(t, errors.Is(directlyNestedErr, io.EOF))
}
