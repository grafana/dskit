package timeutil

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestVariableTicker(t *testing.T) {
	t.Run("should tick at configured durations", func(t *testing.T) {
		t.Parallel()

		startTime := time.Now()
		stop, tickerChan := NewVariableTicker(time.Second, 2*time.Second)
		t.Cleanup(stop)

		// Capture the timing of 3 ticks.
		var ticks []time.Time
		for len(ticks) < 3 {
			ticks = append(ticks, <-tickerChan)
		}

		tolerance := 250 * time.Millisecond
		assert.InDelta(t, ticks[0].Sub(startTime).Seconds(), 1*time.Second.Seconds(), tolerance.Seconds())
		assert.InDelta(t, ticks[1].Sub(startTime).Seconds(), 3*time.Second.Seconds(), tolerance.Seconds())
		assert.InDelta(t, ticks[2].Sub(startTime).Seconds(), 5*time.Second.Seconds(), tolerance.Seconds())
	})

	t.Run("should not close the channel on stop function called", func(t *testing.T) {
		t.Parallel()

		for _, durations := range [][]time.Duration{{time.Second}, {time.Second, 2 * time.Second}} {
			durations := durations

			t.Run(fmt.Sprintf("durations: %v", durations), func(t *testing.T) {
				t.Parallel()

				stop, tickerChan := NewVariableTicker(durations...)
				stop()

				select {
				case <-tickerChan:
					t.Error("should not close the channel and not send any further tick")
				case <-time.After(2 * time.Second):
					// All good.
				}
			})
		}
	})

	t.Run("stop function should be idempotent", func(t *testing.T) {
		t.Parallel()

		for _, durations := range [][]time.Duration{{time.Second}, {time.Second, 2 * time.Second}} {
			durations := durations

			t.Run(fmt.Sprintf("durations: %v", durations), func(t *testing.T) {
				t.Parallel()

				stop, tickerChan := NewVariableTicker(durations...)

				// Call stop() twice.
				stop()
				stop()

				select {
				case <-tickerChan:
					t.Error("should not close the channel and not send any further tick")
				case <-time.After(2 * time.Second):
					// All good.
				}
			})
		}
	})
}
