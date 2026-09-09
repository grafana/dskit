package retry

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConstant(t *testing.T) {
	s := Constant(time.Second)
	for attempts := 1; attempts <= 5; attempts++ {
		assert.Equal(t, time.Second, s(attempts))
	}
}

func TestUniform_clampsItsArguments(t *testing.T) {
	for name, tc := range map[string]struct {
		backoff Backoff
		want    time.Duration
	}{
		"max below min waits exactly min": {Uniform(time.Second, time.Millisecond), time.Second},
		"an empty interval is constant":   {Uniform(time.Second, time.Second), time.Second},
		"a negative min is no wait":       {Uniform(-time.Second, 0), 0},
		// The difference between the bounds is what overflows, so the case to
		// cover is a max far enough below min to wrap it, not merely below it.
		"a max below min by more than a Duration holds": {Uniform(time.Nanosecond, math.MinInt64), time.Nanosecond},
		"both bounds below zero":                        {Uniform(math.MinInt64, math.MinInt64), 0},
	} {
		t.Run(name, func(t *testing.T) {
			for attempts := 1; attempts <= 5; attempts++ {
				assert.Equal(t, tc.want, tc.backoff(attempts))
			}
		})
	}
}

func TestExponential_growsByTheFactor(t *testing.T) {
	s := Exponential(time.Second, 3, time.Hour)

	assert.Equal(t, time.Second, s(1))
	assert.Equal(t, 3*time.Second, s(2))
	assert.Equal(t, 9*time.Second, s(3))
	assert.Equal(t, 27*time.Second, s(4))
}

func TestExponential_clampsAtMax(t *testing.T) {
	s := Exponential(time.Second, 2, 10*time.Second)

	assert.Equal(t, 8*time.Second, s(4))
	assert.Equal(t, 10*time.Second, s(5))
	assert.Equal(t, 10*time.Second, s(500), "an attempt count that overflows the growth still clamps")
}

func TestExponential_zeroMaxLeavesGrowthUnbounded(t *testing.T) {
	s := Exponential(time.Second, 2, 0)
	assert.Equal(t, 1024*time.Second, s(11))
}

func TestExponential_clampsItsArguments(t *testing.T) {
	assert.Equal(t, time.Second, Exponential(time.Second, 0.5, time.Minute)(5),
		"a factor below one stops the delay shrinking")
	assert.Equal(t, time.Second, Exponential(time.Second, math.NaN(), time.Minute)(5),
		"a factor that is not a number never grows the delay")
	assert.Equal(t, time.Minute, Exponential(time.Second, math.Inf(1), time.Minute)(2),
		"an infinite factor reaches the ceiling in one step")
}

func TestExponential_aStartAtOrBelowZeroNeverWaits(t *testing.T) {
	for name, s := range map[string]Backoff{
		"zero":     Exponential(0, 2, time.Minute),
		"negative": Exponential(-time.Second, 2, time.Minute),
	} {
		t.Run(name, func(t *testing.T) {
			for _, attempts := range []int{1, 10, 1_000_000} {
				assert.Zero(t, s(attempts), "a backoff that starts at zero stays at zero")
			}
		})
	}
}

func TestLinear_growsByTheIncrement(t *testing.T) {
	s := Linear(time.Second, 1, time.Hour)

	assert.Equal(t, time.Second, s(1))
	assert.Equal(t, 2*time.Second, s(2))
	assert.Equal(t, 3*time.Second, s(3))
	assert.Equal(t, 4*time.Second, s(4))

	half := Linear(time.Second, 0.5, time.Hour)
	assert.Equal(t, time.Second, half(1))
	assert.Equal(t, 1500*time.Millisecond, half(2))
	assert.Equal(t, 2*time.Second, half(3))
}

func TestLinear_clampsAtMax(t *testing.T) {
	s := Linear(time.Second, 1, 3*time.Second)

	assert.Equal(t, 3*time.Second, s(3))
	assert.Equal(t, 3*time.Second, s(4))
	assert.Equal(t, 3*time.Second, s(1_000_000))
}

func TestLinear_clampsItsArguments(t *testing.T) {
	for name, s := range map[string]Backoff{
		"a factor of zero never grows":              Linear(time.Second, 0, time.Minute),
		"a negative factor never grows":             Linear(time.Second, -1, time.Minute),
		"a factor that is not a number never grows": Linear(time.Second, math.NaN(), time.Minute),
		"a count below one waits start":             Linear(time.Second, 2, time.Minute),
		"an infinite factor waits start first":      Linear(time.Second, math.Inf(1), time.Minute),
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, time.Second, s(1))
			assert.Equal(t, time.Second, s(0), "a count below the documented minimum stays positive")
			assert.Equal(t, time.Second, s(-5))
		})
	}

	assert.Equal(t, time.Minute, Linear(time.Second, math.Inf(1), time.Minute)(2),
		"an infinite factor reaches the ceiling in one step")
}

func TestLinear_aStartAtOrBelowZeroNeverWaits(t *testing.T) {
	for name, s := range map[string]Backoff{
		"zero":     Linear(0, 1, time.Minute),
		"negative": Linear(-time.Second, 1, time.Minute),
	} {
		t.Run(name, func(t *testing.T) {
			for _, attempts := range []int{1, 10, 1_000_000} {
				assert.Zero(t, s(attempts))
			}
		})
	}
}

func TestJittered_onlySubtracts(t *testing.T) {
	const ceiling = 10 * time.Second
	s := Jittered(Exponential(time.Second, 2, ceiling), 0.5)

	// Attempt 5 and beyond sit on the ceiling, where the jitter has to survive.
	var low, high time.Duration = ceiling, 0
	for range 2000 {
		delay := s(5)
		require.LessOrEqual(t, delay, ceiling, "jitter must never push a delay above the backoff's max")
		low, high = min(low, delay), max(high, delay)
	}

	assert.Less(t, low, 6*time.Second, "the window should reach down to half of max")
	assert.Greater(t, high, 9*time.Second, "the window should reach up to max")
}

func TestJittered_fullFractionReachesZero(t *testing.T) {
	s := Jittered(Constant(time.Second), 1)

	low := time.Second
	for range 2000 {
		delay := s(1)
		require.LessOrEqual(t, delay, time.Second)
		low = min(low, delay)
	}
	assert.Less(t, low, 100*time.Millisecond, "a fraction of 1 draws from the whole interval below the delay")
}

func TestJittered_clampsItsFraction(t *testing.T) {
	assert.Equal(t, time.Second, Jittered(Constant(time.Second), 0)(1), "a fraction of zero is no jitter")
	assert.Equal(t, time.Second, Jittered(Constant(time.Second), -1)(1), "a negative fraction is no jitter")
	assert.Equal(t, time.Second, Jittered(Constant(time.Second), math.NaN())(1),
		"a fraction that is not a number is no jitter")

	for name, s := range map[string]Backoff{
		"above one": Jittered(Constant(time.Second), 5),
		"infinite":  Jittered(Constant(time.Second), math.Inf(1)),
	} {
		t.Run(name, func(t *testing.T) {
			for range 100 {
				delay := s(1)
				assert.GreaterOrEqual(t, delay, time.Duration(0), "a fraction above one cannot go negative")
				assert.LessOrEqual(t, delay, time.Second)
			}
		})
	}
}

func TestJittered_leavesANonPositiveDelayAlone(t *testing.T) {
	assert.Zero(t, Jittered(Constant(0), 1)(1))
	assert.Equal(t, -time.Second, Jittered(Constant(-time.Second), 1)(1),
		"a negative delay is passed through rather than jittered towards zero")
}

func TestJittered_appliesToAnyBackoff(t *testing.T) {
	s := Jittered(Uniform(time.Second, 2*time.Second), 0.5)

	for range 500 {
		delay := s(1)
		require.GreaterOrEqual(t, delay, 500*time.Millisecond, "at most half of the smallest delay is taken")
		require.Less(t, delay, 2*time.Second)
	}
}

// TestBackoff_extremeArgumentsStayInRange sweeps every Backoff over the
// arithmetic edges of its inputs, where an out-of-range conversion would
// otherwise yield a negative delay and turn a wait into a hot loop.
func TestBackoff_extremeArgumentsStayInRange(t *testing.T) {
	const maxDelay = time.Duration(math.MaxInt64)

	type backoffCase struct {
		name    string
		backoff Backoff
		ceiling time.Duration // zero when the growth is unbounded
	}

	cases := []backoffCase{
		{"exponential", Exponential(time.Second, 2, 0), 0},
		{"exponential capped", Exponential(time.Second, 2, time.Minute), time.Minute},
		{"exponential from the longest delay", Exponential(maxDelay, 2, 0), 0},
		{"exponential capped at the longest delay", Exponential(time.Second, 2, maxDelay), maxDelay},
		{"exponential with a huge factor", Exponential(time.Second, math.MaxFloat64, 0), 0},
		{"exponential with an infinite factor", Exponential(time.Second, math.Inf(1), 0), 0},
		{"exponential with an unordered factor", Exponential(time.Second, math.NaN(), 0), 0},
		{"linear", Linear(time.Hour, 1, 0), 0},
		{"linear capped", Linear(time.Hour, 1, time.Minute), time.Minute},
		{"linear from the longest delay", Linear(maxDelay, 1, 0), 0},
		{"linear with a huge factor", Linear(time.Second, math.MaxFloat64, 0), 0},
		{"linear with an infinite factor", Linear(time.Second, math.Inf(1), 0), 0},
		{"linear with an unordered factor", Linear(time.Second, math.NaN(), 0), 0},
		{"uniform over the whole range", Uniform(0, maxDelay), maxDelay},
		{"uniform with its bounds reversed", Uniform(maxDelay, math.MinInt64), maxDelay},
		{"constant at the longest delay", Constant(maxDelay), maxDelay},
	}

	// Jitter is the last thing applied to a delay, so every shape is swept
	// through it as well.
	for _, tc := range cases[:len(cases):len(cases)] {
		cases = append(cases,
			backoffCase{"jittered " + tc.name, Jittered(tc.backoff, 1), tc.ceiling},
			backoffCase{"unordered-jittered " + tc.name, Jittered(tc.backoff, math.NaN()), tc.ceiling},
		)
	}

	// Attempt counts a caller could reach under WithUnlimitedAttempts, plus the
	// ones below the documented minimum of 1. attempts-1 wraps at MinInt64.
	attempts := []int{math.MinInt64, -5, 0, 1, 2, 63, 64, 65, 1_000_000, math.MaxInt32, math.MaxInt64}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, n := range attempts {
				for range 10 {
					delay := tc.backoff(n)
					require.GreaterOrEqual(t, delay, time.Duration(0),
						"attempt %d produced a negative delay", n)
					if tc.ceiling > 0 {
						require.LessOrEqual(t, delay, tc.ceiling,
							"attempt %d exceeded the ceiling", n)
					}
				}
			}
		})
	}
}

func TestBackoff_isSafeForConcurrentUse(t *testing.T) {
	s := Jittered(Exponential(time.Millisecond, 2, time.Second), 0.5)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempts := 1; attempts <= 100; attempts++ {
				assert.LessOrEqual(t, s(attempts), time.Second)
			}
		}()
	}
	wg.Wait()
}
