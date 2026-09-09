package retry

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// metricsWithClock builds the metrics against a test clock.
func metricsWithClock(reg prometheus.Registerer, now func() time.Time) *Metrics {
	m := NewMetrics(reg)
	m.now = now
	return m
}

// stepClock returns a clock that starts at a fixed instant and advances by step
// on every read. Retry reads it exactly twice, once at each end of the loop, so
// every call measures exactly step, however long the test really took. It is
// safe to share across calls, but only sequential ones: concurrent readers would
// interleave their pairs and measure multiples of step.
func stepClock(step time.Duration) func() time.Time {
	var mu sync.Mutex
	now := time.Unix(0, 0)
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		read := now
		now = now.Add(step)
		return read
	}
}

func TestMetrics_outcomes(t *testing.T) {
	for name, tc := range map[string]struct {
		failures int
		err      error
		opts     []Option
		attempts int
		outcome  string
	}{
		"first attempt": {
			failures: 0,
			attempts: 1,
			outcome:  outcomeFirstAttempt,
		},
		"after retry": {
			failures: 2,
			err:      MarkRetryable(errBoom),
			attempts: 3,
			outcome:  outcomeAfterRetry,
		},
		"terminal": {
			failures: 1,
			err:      errBoom,
			attempts: 1,
			outcome:  outcomeTerminal,
		},
		"exhausted": {
			failures: 10,
			err:      MarkRetryable(errBoom),
			attempts: 3,
			outcome:  outcomeExhausted,
		},
	} {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			fn, _ := failFor(tc.failures, tc.err)

			_ = Retry(context.Background(), fn, append(tc.opts,
				WithBackoff(Constant(0)), WithMaxAttempts(3),
				WithMetrics(metricsWithClock(reg, stepClock(time.Second))),
				WithOperation("sync"),
			)...)

			assert.Equal(t, float64(tc.attempts), counterValue(t, reg, "retry_attempts_total"))

			duration := single(t, reg, "retry_duration_seconds")
			assert.Equal(t, map[string]string{"operation": "sync", "outcome": tc.outcome},
				labelsOf(duration), "every call reports exactly one outcome")
			assert.Equal(t, uint64(1), duration.GetHistogram().GetSampleCount(),
				"one observation per completed operation, so _count counts operations")
			assert.Equal(t, float64(1), duration.GetHistogram().GetSampleSum(),
				"seconds, and the whole span of the loop")
		})
	}
}

// TestMetrics_durationIsSeconds pins the histogram in full: the unit, which a
// _sum alone would let drift by a factor of a billion, and the buckets, which
// callers write alert thresholds against.
func TestMetrics_durationIsSeconds(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	fn, _ := failFor(0, nil)

	_ = Retry(context.Background(), fn,
		WithMetrics(metricsWithClock(reg, stepClock(2500*time.Millisecond))),
		WithOperation("sync"),
	)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP retry_duration_seconds Time spent on completed operations, waits between attempts included, by final outcome.
		# TYPE retry_duration_seconds histogram
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="0.001"} 0
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="0.01"} 0
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="0.1"} 0
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="0.5"} 0
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="1"} 0
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="2.5"} 1
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="5"} 1
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="10"} 1
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="30"} 1
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="60"} 1
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="120"} 1
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="300"} 1
		retry_duration_seconds_bucket{operation="sync",outcome="first_attempt",le="+Inf"} 1
		retry_duration_seconds_sum{operation="sync",outcome="first_attempt"} 2.5
		retry_duration_seconds_count{operation="sync",outcome="first_attempt"} 1
	`), "retry_duration_seconds"))
}

// TestMetrics_durationCoversTheWaits is the reason the metric exists: the delay
// between attempts is the caller's latency, and no amount of reading the backoff
// config recovers it once Jittered and Decision.After have had their say.
func TestMetrics_durationCoversTheWaits(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	fn, calls := failFor(1, MarkRetryableAfter(errBoom, 20*time.Millisecond))

	// A real clock here, because the wait being measured is a real one.
	require.NoError(t, Retry(context.Background(), fn,
		WithBackoff(Constant(0)), WithMaxAttempts(3),
		WithMetrics(NewMetrics(reg)),
	))
	require.Equal(t, 2, *calls)

	assert.GreaterOrEqual(t, single(t, reg, "retry_duration_seconds").GetHistogram().GetSampleSum(), 0.02,
		"the server-supplied floor was waited out inside the measured span")
}

func TestMetrics_cancelledOutcome(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = Retry(ctx, func(context.Context) error { return nil }, WithMetrics(NewMetrics(reg)))

	duration := single(t, reg, "retry_duration_seconds")
	assert.Equal(t, map[string]string{"operation": "unknown", "outcome": "cancelled"}, labelsOf(duration))
	assert.Equal(t, uint64(1), duration.GetHistogram().GetSampleCount(),
		"a loop that never ran an attempt still reports how it ended")
}

func TestMetrics_nilIsSilent(t *testing.T) {
	fn, _ := failFor(1, MarkRetryable(errBoom))

	assert.NoError(t, Retry(context.Background(), fn, WithBackoff(Constant(0)), WithMaxAttempts(3), WithMetrics(nil)))
}

func counterValue(t *testing.T, reg prometheus.Gatherer, name string) float64 {
	t.Helper()
	return single(t, reg, name).GetCounter().GetValue()
}

// outcomeCount is the number of operations that ended in outcome, read off the
// histogram's _count.
func outcomeCount(t *testing.T, reg prometheus.Gatherer, operation, outcome string) uint64 {
	t.Helper()

	for _, metric := range family(t, reg, "retry_duration_seconds").GetMetric() {
		labels := labelsOf(metric)
		if labels["operation"] == operation && labels["outcome"] == outcome {
			return metric.GetHistogram().GetSampleCount()
		}
	}
	return 0
}

func labelsOf(metric *dto.Metric) map[string]string {
	labels := make(map[string]string, len(metric.GetLabel()))
	for _, pair := range metric.GetLabel() {
		labels[pair.GetName()] = pair.GetValue()
	}
	return labels
}

func single(t *testing.T, reg prometheus.Gatherer, name string) *dto.Metric {
	t.Helper()

	metrics := family(t, reg, name).GetMetric()
	require.Len(t, metrics, 1)
	return metrics[0]
}

func family(t *testing.T, reg prometheus.Gatherer, name string) *dto.MetricFamily {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	t.Fatalf("metric %s not found", name)
	return nil
}
