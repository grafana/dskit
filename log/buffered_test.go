// Provenance-includes-location: https://github.com/grafana/loki/blob/7c78d7ea44afb420847255f9f5a4f677ad0f47bf/pkg/util/log/line_buffer_test.go
// Provenance-includes-location: https://github.com/grafana/mimir/blob/c8b24a462f7e224950409e7e0a4e0a58f3a79599/pkg/util/log/line_buffer_test.go
// Provenance-includes-copyright: Grafana Labs
package log

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	flushPeriod = 10 * time.Millisecond
	bufferSize  = 10e6
)

// BenchmarkBuffered creates buffered loggers of various capacities to see which perform best.
func BenchmarkBuffered(b *testing.B) {
	for i := 1; i <= 2048; i *= 2 {
		f := outFile(b)

		bufLog := NewBufferedLogger(f, uint32(i),
			WithFlushPeriod(flushPeriod),
			WithPrellocatedBuffer(bufferSize),
		)
		l := log.NewLogfmtLogger(bufLog)

		b.Run(fmt.Sprintf("capacity:%d", i), func(b *testing.B) {
			b.ReportAllocs()
			b.StartTimer()

			require.NoError(b, f.Truncate(0))

			logger := log.With(l, "common_key", "common_value")
			for j := 0; j < b.N; j++ {
				logger.Log("foo_key", "foo_value")
			}

			// force a final flush for outstanding lines in buffer
			bufLog.Flush()
			b.StopTimer()

			contents, err := os.ReadFile(f.Name())
			require.NoErrorf(b, err, "could not read test file: %s", f.Name())

			lines := strings.Split(string(contents), "\n")
			require.Len(b, lines, b.N+1)
		})
	}
}

// BenchmarkUnbuffered should perform roughly equivalently to a buffered logger with a capacity of 1.
func BenchmarkUnbuffered(b *testing.B) {
	b.ReportAllocs()

	f := outFile(b)

	l := log.NewLogfmtLogger(f)
	benchmarkRunner(b, l, baseMessage)

	b.StopTimer()

	contents, err := os.ReadFile(f.Name())
	require.NoErrorf(b, err, "could not read test file: %s", f.Name())

	lines := strings.Split(string(contents), "\n")
	require.Len(b, lines, b.N+1)
}

func BenchmarkLineDiscard(b *testing.B) {
	b.ReportAllocs()

	l := log.NewLogfmtLogger(io.Discard)
	benchmarkRunner(b, l, baseMessage)
}

func TestBufferedConcurrency(t *testing.T) {
	t.Parallel()
	bufLog := NewBufferedLogger(io.Discard, 32,
		WithFlushPeriod(flushPeriod),
		WithPrellocatedBuffer(bufferSize),
	)
	testConcurrency(t, log.NewLogfmtLogger(bufLog), 10000)
}

func TestOnFlushCallback(t *testing.T) {
	var (
		flushCount     uint32
		flushedEntries int
		buf            bytes.Buffer
	)

	callback := func(entries uint32) {
		flushCount++
		flushedEntries += int(entries)
	}

	// Do NOT specify a flush period because we want to be in control of the flushes done by this test.
	bufLog := NewBufferedLogger(&buf, 2,
		WithPrellocatedBuffer(bufferSize),
		WithFlushCallback(callback),
	)

	l := log.NewLogfmtLogger(bufLog)
	require.NoError(t, l.Log("line"))
	require.NoError(t, l.Log("line"))
	// first flush
	require.NoError(t, l.Log("line"))

	// pre-condition check: the last Log() call should have flushed previous entries.
	require.Equal(t, uint32(1), bufLog.Size())

	// force a second
	require.NoError(t, bufLog.Flush())

	require.Equal(t, uint32(2), flushCount)
	require.Len(t, strings.Split(buf.String(), "\n"), flushedEntries+1)
}

// outFile creates a real OS file for testing.
// We cannot use stdout/stderr since we need to read the contents afterwards to validate, and we have to write to a file
// to benchmark the impact of write() syscalls.
func outFile(b *testing.B) *os.File {
	f, err := os.CreateTemp(b.TempDir(), "buffered*")
	require.NoErrorf(b, err, "cannot create test file")

	return f
}

// Copied from go-kit/log
// These tests are designed to be run with the race detector.

func testConcurrency(t *testing.T, logger log.Logger, total int) {
	n := int(math.Sqrt(float64(total)))
	share := total / n

	var g errgroup.Group
	for i := 0; i < n; i++ {
		g.Go(func() error {
			for i := 0; i < share; i++ {
				if err := logger.Log("key", i); err != nil {
					return err
				}
			}
			return nil
		})
	}

	require.NoError(t, g.Wait(), "concurrent logging error")
}

func benchmarkRunner(b *testing.B, logger log.Logger, f func(log.Logger)) {
	lc := log.With(logger, "common_key", "common_value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f(lc)
	}
}

func baseMessage(logger log.Logger) {
	logger.Log("foo_key", "foo_value")
}
