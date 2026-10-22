package retry

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The values of the outcome label on retry_duration_seconds. Every call to Retry
// reports exactly one of them.
const (
	outcomeFirstAttempt = "first_attempt" // fn succeeded straight away
	outcomeAfterRetry   = "after_retry"   // fn succeeded, but not on the first attempt
	outcomeTerminal     = "terminal"      // the decider gave up on a failure
	outcomeExhausted    = "exhausted"     // the attempt budget ran out
	outcomeDeadline     = "deadline"      // the next delay would have outlasted the deadline
	outcomeCancelled    = "cancelled"     // the context ended

	unknownOperation = "unknown"
)

// durationBuckets spans a retry loop rather than a single request: the bottom
// bucket is the fast path, where fn succeeded first time and nothing was waited
// out, and the top is a loop that spent minutes on a dependency that never came
// back. The default backoff reaches roughly 32s across its ten attempts.
var durationBuckets = []float64{0.001, 0.01, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

// Metrics instruments retries. Build it once per registry with NewMetrics,
// because Prometheus panics on duplicate registration, and pass it to each
// Retry call with WithMetrics. A nil *Metrics records nothing.
//
// Read retry_duration_seconds_count as a count of operations by how they ended:
// the ratio after_retry / (after_retry + exhausted) answers the question worth
// asking of any retry loop, which is whether retrying is helping. The buckets
// answer what it costs, in latency the caller cannot measure for itself.
type Metrics struct {
	attempts *prometheus.CounterVec
	duration *prometheus.HistogramVec

	// now reads the clock the durations are measured against.
	now func() time.Time
}

// NewMetrics registers the retry metrics on reg. Metric names are
// unprefixed, so callers own the prefix via
// prometheus.WrapRegistererWithPrefix.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	return &Metrics{
		attempts: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "retry_attempts_total",
			Help: "Total number of attempts made, including the first attempt of each operation.",
		}, []string{"operation"}),
		duration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "retry_duration_seconds",
			Help:    "Time spent on completed operations, waits between attempts included, by final outcome.",
			Buckets: durationBuckets,
			// Use defaults recommended by Prometheus for native histograms.
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, []string{"operation", "outcome"}),
		now: time.Now,
	}
}

func (m *Metrics) observeAttempt(operation string) {
	if m == nil {
		return
	}
	m.attempts.WithLabelValues(operation).Inc()
}

// startTime reads the instant an operation begins, and the zero Time when there
// is nothing to report it to, so that the clock is not read for nobody.
func (m *Metrics) startTime() time.Time {
	if m == nil {
		return time.Time{}
	}
	return m.now()
}

// observeOutcome records how one operation ended and how long it took, counting
// from the startTime it began at. Exactly one observation per completed
// operation, so the histogram's _count is also the number of operations that
// ended in that outcome.
func (m *Metrics) observeOutcome(operation, outcome string, startTime time.Time) {
	if m == nil {
		return
	}
	m.duration.WithLabelValues(operation, outcome).Observe(m.now().Sub(startTime).Seconds())
}
