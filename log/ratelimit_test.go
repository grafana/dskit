package log

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimitedLoggerLogs(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	c := newCounterLogger(buf)
	reg := prometheus.NewPedanticRegistry()
	r := NewRateLimitedLogger(c, 1, 1, reg)

	level.Error(r).Log("msg", "error will be logged")
	assert.Equal(t, 1, c.count)

	logContains := []string{"error", "error will be logged"}
	c.assertContains(t, logContains)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP logger_rate_limit_discarded_log_lines_total Total number of discarded log lines per level.
		# TYPE logger_rate_limit_discarded_log_lines_total counter
        logger_rate_limit_discarded_log_lines_total{level="info"} 0
        logger_rate_limit_discarded_log_lines_total{level="debug"} 0
        logger_rate_limit_discarded_log_lines_total{level="warn"} 0
        logger_rate_limit_discarded_log_lines_total{level="error"} 0
	`)))
}

func TestRateLimitedLoggerLimits(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	c := newCounterLogger(buf)
	reg := prometheus.NewPedanticRegistry()
	r := NewRateLimitedLogger(c, 2, 2, reg)

	level.Error(r).Log("msg", "error 1 will be logged")
	assert.Equal(t, 1, c.count)
	c.assertContains(t, []string{"error", "error 1 will be logged"})

	level.Info(r).Log("msg", "info 1 will be logged")
	assert.Equal(t, 2, c.count)
	c.assertContains(t, []string{"info", "info 1 will be logged"})

	level.Debug(r).Log("msg", "debug 1 will be discarded")
	assert.Equal(t, 2, c.count)
	c.assertNotContains(t, "debug 1 will be discarded")

	level.Warn(r).Log("msg", "warning 1 will be discarded")
	assert.Equal(t, 2, c.count)
	c.assertNotContains(t, "warning 1 will be discarded")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP logger_rate_limit_discarded_log_lines_total Total number of discarded log lines per level.
		# TYPE logger_rate_limit_discarded_log_lines_total counter
        logger_rate_limit_discarded_log_lines_total{level="info"} 0
        logger_rate_limit_discarded_log_lines_total{level="debug"} 1
        logger_rate_limit_discarded_log_lines_total{level="warn"} 1
        logger_rate_limit_discarded_log_lines_total{level="error"} 0
	`)))

	// we wait 1 second, so the next group of lines can be logged
	time.Sleep(time.Second)
	level.Debug(r).Log("msg", "debug 2 will be logged")
	assert.Equal(t, 3, c.count)
	c.assertContains(t, []string{"debug", "debug 2 will be logged"})

	level.Info(r).Log("msg", "info 2 will be logged")
	assert.Equal(t, 4, c.count)
	c.assertContains(t, []string{"info", "info 2 will be logged"})

	level.Error(r).Log("msg", "error 2 will be discarded")
	assert.Equal(t, 4, c.count)
	c.assertNotContains(t, "error 2 will be discarded")

	level.Warn(r).Log("msg", "warning 2 will be discarded")
	assert.Equal(t, 4, c.count)
	c.assertNotContains(t, "warning 2 will be discarded")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP logger_rate_limit_discarded_log_lines_total Total number of discarded log lines per level.
		# TYPE logger_rate_limit_discarded_log_lines_total counter
        logger_rate_limit_discarded_log_lines_total{level="info"} 0
        logger_rate_limit_discarded_log_lines_total{level="debug"} 1
        logger_rate_limit_discarded_log_lines_total{level="error"} 1
        logger_rate_limit_discarded_log_lines_total{level="warn"} 2
	`)))
}

func TestRateLimitedLoggerWithFields(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	c := newCounterLogger(buf)
	reg := prometheus.NewPedanticRegistry()
	logger := NewRateLimitedLogger(c, 1, 1, reg)
	loggerWithFields := log.With(logger, "key", "value")

	level.Error(loggerWithFields).Log("msg", "error will be logged")
	assert.Equal(t, 1, c.count)
	c.assertContains(t, []string{"key", "value", "error", "error will be logged"})

	level.Info(logger).Log("msg", "info will not be logged")
	c.assertNotContains(t, "info will not be logged")

	level.Debug(loggerWithFields).Log("msg", "debug will not be logged")
	c.assertNotContains(t, "debug will not be logged")

	level.Warn(loggerWithFields).Log("msg", "warning will not be logged")
	c.assertNotContains(t, "warning will not be logged")
	assert.Equal(t, 1, c.count)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP logger_rate_limit_discarded_log_lines_total Total number of discarded log lines per level.
		# TYPE logger_rate_limit_discarded_log_lines_total counter
        logger_rate_limit_discarded_log_lines_total{level="info"} 1
        logger_rate_limit_discarded_log_lines_total{level="debug"} 1
        logger_rate_limit_discarded_log_lines_total{level="warn"} 1
        logger_rate_limit_discarded_log_lines_total{level="error"} 0
	`)))
}

type counterLogger struct {
	logger log.Logger
	buf    *bytes.Buffer
	count  int
}

func (c *counterLogger) Log(keyvals ...interface{}) error {
	err := c.logger.Log(keyvals...)
	if err == nil {
		c.count++
	}
	return err
}

func (c *counterLogger) assertContains(t *testing.T, logContains []string) {
	for _, content := range logContains {
		require.True(t, bytes.Contains(c.buf.Bytes(), []byte(content)))
	}
}

func (c *counterLogger) assertNotContains(t *testing.T, content string) {
	require.False(t, bytes.Contains(c.buf.Bytes(), []byte(content)))
}

func newCounterLogger(buf *bytes.Buffer) *counterLogger {
	return &counterLogger{
		logger: log.NewLogfmtLogger(buf),
		buf:    buf,
	}
}
