// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/logging/gokit_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package log

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func BenchmarkLazySprintf(b *testing.B) {
	g := log.NewNopLogger()
	logger := level.NewFilter(addStandardFields(g), level.AllowInfo())
	// Simulate the parameters used in middleware/logging.go
	var (
		method     = "method"
		uri        = "https://example.com/foobar"
		statusCode = 404
		duration   = 42
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		level.Debug(logger).Log("msg", LazySprintf("%s %s (%d) %v", method, uri, statusCode, duration))
	}
}

func TestLazySprintf(t *testing.T) {
	tests := map[string]struct {
		id     int
		lvl    string
		format string
	}{
		"debug level should log debug messages and call Sprintf": {
			id:     1,
			lvl:    "debug",
			format: "debug %d has been logged %v",
		},
		"info level should not log debug messages and should not call Sprintf": {
			id:     2,
			lvl:    "info",
			format: "info %d has not been logged %v",
		},
	}

	buf := bytes.NewBuffer(nil)

	for _, test := range tests {
		buf.Reset()
		logger := NewGoKitWithWriter(LogfmtFormat, buf)
		var lvl Level
		require.NoError(t, lvl.Set(test.lvl))
		logger = level.NewFilter(logger, lvl.Option)
		now := time.Now()
		expectedMessage := fmt.Sprintf(test.format, test.id, now)
		lazySprintf := newLazySprintfWithCount("debug %d has been logged %v", test.id, now)
		level.Debug(logger).Log("msg", lazySprintf)
		if test.lvl == "debug" {
			require.True(t, bytes.Contains(buf.Bytes(), []byte(expectedMessage)))
			require.Equal(t, 1, lazySprintf.count)
		} else {
			require.False(t, bytes.Contains(buf.Bytes(), []byte(expectedMessage)))
			require.Equal(t, 0, lazySprintf.count)
		}
	}
}

func TestNewGoKitWithRateLimitedAndFields(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	var lvl Level
	require.NoError(t, lvl.Set("info"))
	now := log.DefaultTimestampUTC()
	logger := NewGoKitWithWriter(LogfmtFormat, buf)

	rateLimitedLogger := NewRateLimitedLogger(logger, 1, 2, prometheus.DefaultRegisterer)

	rateLimitedLogger = log.With(rateLimitedLogger, "ts", now, "caller", log.Caller(5))
	rateLimitedLogger = level.NewFilter(rateLimitedLogger, lvl.Option)

	for i := 0; i < 1000; i++ {
		level.Info(rateLimitedLogger).Log("msg", LazySprintf("info %d", i))
		level.Debug(rateLimitedLogger).Log("msg", LazySprintf("debug %d", i))
	}

	format := "ts=%s caller=gokit_test.go:%d level=%s msg=\"info %d\""
	for i := 0; i < 1000; i++ {
		if i < 2 {
			require.True(t, bytes.Contains(buf.Bytes(), []byte(fmt.Sprintf(format, now, 88, "info", i))))
		} else {
			require.False(t, bytes.Contains(buf.Bytes(), []byte(fmt.Sprintf(format, now, 88, "info", i))))
		}

		require.False(t, bytes.Contains(buf.Bytes(), []byte(fmt.Sprintf(format, now, 89, "debug", i))))
	}
}

type lazySprintfWithCount struct {
	next  Sprintf
	count int
}

func newLazySprintfWithCount(format string, args ...interface{}) *lazySprintfWithCount {
	return &lazySprintfWithCount{
		Sprintf{
			format: format,
			args:   args,
		},
		0,
	}
}

func (f *lazySprintfWithCount) String() string {
	f.count++
	return f.next.String()
}
