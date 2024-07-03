package backoff

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestBackoff_NextDelay(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		minBackoff     time.Duration
		maxBackoff     time.Duration
		expectedRanges [][]time.Duration
	}{
		"exponential backoff with jitter honoring min and max": {
			minBackoff: 100 * time.Millisecond,
			maxBackoff: 10 * time.Second,
			expectedRanges: [][]time.Duration{
				{100 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 400 * time.Millisecond},
				{400 * time.Millisecond, 800 * time.Millisecond},
				{800 * time.Millisecond, 1600 * time.Millisecond},
				{1600 * time.Millisecond, 3200 * time.Millisecond},
				{3200 * time.Millisecond, 6400 * time.Millisecond},
				{6400 * time.Millisecond, 10000 * time.Millisecond},
				{6400 * time.Millisecond, 10000 * time.Millisecond},
			},
		},
		"exponential backoff with max equal to the end of a range": {
			minBackoff: 100 * time.Millisecond,
			maxBackoff: 800 * time.Millisecond,
			expectedRanges: [][]time.Duration{
				{100 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 400 * time.Millisecond},
				{400 * time.Millisecond, 800 * time.Millisecond},
				{400 * time.Millisecond, 800 * time.Millisecond},
			},
		},
		"exponential backoff with max equal to the end of a range + 1": {
			minBackoff: 100 * time.Millisecond,
			maxBackoff: 801 * time.Millisecond,
			expectedRanges: [][]time.Duration{
				{100 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 400 * time.Millisecond},
				{400 * time.Millisecond, 800 * time.Millisecond},
				{800 * time.Millisecond, 801 * time.Millisecond},
				{800 * time.Millisecond, 801 * time.Millisecond},
			},
		},
		"exponential backoff with max equal to the end of a range - 1": {
			minBackoff: 100 * time.Millisecond,
			maxBackoff: 799 * time.Millisecond,
			expectedRanges: [][]time.Duration{
				{100 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 400 * time.Millisecond},
				{400 * time.Millisecond, 799 * time.Millisecond},
				{400 * time.Millisecond, 799 * time.Millisecond},
			},
		},
		"min backoff is equal to max": {
			minBackoff: 100 * time.Millisecond,
			maxBackoff: 100 * time.Millisecond,
			expectedRanges: [][]time.Duration{
				{100 * time.Millisecond, 100 * time.Millisecond},
				{100 * time.Millisecond, 100 * time.Millisecond},
				{100 * time.Millisecond, 100 * time.Millisecond},
			},
		},
		"min backoff is greater then max": {
			minBackoff: 200 * time.Millisecond,
			maxBackoff: 100 * time.Millisecond,
			expectedRanges: [][]time.Duration{
				{200 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 200 * time.Millisecond},
			},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			b := New(context.Background(), Config{
				MinBackoff: testData.minBackoff,
				MaxBackoff: testData.maxBackoff,
				MaxRetries: len(testData.expectedRanges),
			})

			for _, expectedRange := range testData.expectedRanges {
				delay := b.NextDelay()

				if delay < expectedRange[0] || delay > expectedRange[1] {
					t.Errorf("%d expected to be within %d and %d", delay, expectedRange[0], expectedRange[1])
				}
			}
		})
	}
}

func TestBackoff_Err(t *testing.T) {
	cause := errors.New("my cause")

	tests := map[string]struct {
		ctx         func(*testing.T) context.Context
		expectedErr error
	}{
		"should return context.DeadlineExceeded when context deadline exceeded without cause": {
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now())
				t.Cleanup(cancel)

				return ctx
			},
			expectedErr: context.DeadlineExceeded,
		},
		"should return cause when context deadline exceeded with cause": {
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithDeadlineCause(context.Background(), time.Now(), cause)
				t.Cleanup(cancel)

				return ctx
			},
			expectedErr: cause,
		},
		"should return context.Canceled when context is canceled without cause": {
			ctx: func(_ *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				return ctx
			},
			expectedErr: context.Canceled,
		},
		"should return cause when context is canceled with cause": {
			ctx: func(_ *testing.T) context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(cause)

				return ctx
			},
			expectedErr: cause,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			b := New(testData.ctx(t), Config{})

			// Wait until the backoff returns error.
			require.Eventually(t, func() bool {
				return b.Err() != nil
			}, time.Second, 10*time.Millisecond)

			require.Equal(t, testData.expectedErr, b.Err())
		})
	}
}
