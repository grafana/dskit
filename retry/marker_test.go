package retry

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarked(t *testing.T) {
	sentinel := errors.New("boom")

	for name, tc := range map[string]struct {
		err  error
		want Decision
	}{
		"nil error": {
			err: nil,
		},
		"unmarked error": {
			err: sentinel,
		},
		"retryable": {
			err:  MarkRetryable(sentinel),
			want: Decision{Retryable: true},
		},
		"retryable after": {
			err:  MarkRetryableAfter(sentinel, 3*time.Second),
			want: Decision{Retryable: true, After: 3 * time.Second},
		},
		"mark survives wrapping": {
			err:  fmt.Errorf("context: %w", MarkRetryable(sentinel)),
			want: Decision{Retryable: true},
		},
		"outermost After wins": {
			err:  MarkRetryableAfter(fmt.Errorf("context: %w", MarkRetryableAfter(sentinel, time.Hour)), time.Second),
			want: Decision{Retryable: true, After: time.Second},
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, Marked(tc.err))
		})
	}
}

func TestMarked_aggregatesAreDecidedByMarkingThem(t *testing.T) {
	joined := errors.Join(MarkRetryableAfter(errors.New("slow"), time.Hour), MarkRetryable(errors.New("transient")))

	assert.True(t, Marked(joined).Retryable, "some mark is found, but which one is unspecified")
	assert.Equal(t, Decision{Retryable: true, After: time.Second}, Marked(MarkRetryableAfter(joined, time.Second)),
		"marking the aggregate decides for the whole of it")
}

func TestMarked_nilErrorIsNotMarked(t *testing.T) {
	assert.NoError(t, MarkRetryable(nil))
	assert.NoError(t, MarkRetryableAfter(nil, time.Second))
}

func TestMarked_preservesTheError(t *testing.T) {
	sentinel := errors.New("boom")
	marked := MarkRetryable(fmt.Errorf("writing: %w", sentinel))

	require.EqualError(t, marked, "writing: boom")
	assert.ErrorIs(t, marked, sentinel)

	var target *statusError
	assert.ErrorAs(t, MarkRetryable(&statusError{code: 404}), &target)
	assert.Equal(t, 404, target.code)
}
