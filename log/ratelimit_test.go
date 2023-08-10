package log

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimitedLoggerLogs(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	c := newCounterLogger(buf)
	reg := prometheus.NewPedanticRegistry()
	r := NewRateLimitedLogger(c, 1, 1, reg)

	r.Errorln("Error will be logged")
	assert.Equal(t, 1, c.count)

	logContains := []string{"error", "Error will be logged"}
	c.assertContains(t, logContains)
}

func TestRateLimitedLoggerLimits(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	c := newCounterLogger(buf)
	reg := prometheus.NewPedanticRegistry()
	r := NewRateLimitedLogger(c, 2, 2, reg)

	r.Errorln("error 1 will be logged")
	assert.Equal(t, 1, c.count)
	c.assertContains(t, []string{"error", "error 1 will be logged"})

	r.Infoln("info 1 will be logged")
	assert.Equal(t, 2, c.count)
	c.assertContains(t, []string{"info", "info 1 will be logged"})

	r.Debugln("debug 1 will be discarded")
	assert.Equal(t, 2, c.count)
	c.assertNotContains(t, "debug 1 will be discarded")

	r.Warnln("warning 1 will be discarded")
	assert.Equal(t, 2, c.count)
	c.assertNotContains(t, "warning 1 will be discarded")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP logger_rate_limit_discarded_log_lines_total Total number of discarded log lines per level.
		# TYPE logger_rate_limit_discarded_log_lines_total counter
        logger_rate_limit_discarded_log_lines_total{level="info"} 0
        logger_rate_limit_discarded_log_lines_total{level="debug"} 1
        logger_rate_limit_discarded_log_lines_total{level="warning"} 1
        logger_rate_limit_discarded_log_lines_total{level="error"} 0
	`)))

	// we wait 1 second, so the next group of lines can be logged
	time.Sleep(time.Second)
	r.Debugln("debug 2 will be logged")
	assert.Equal(t, 3, c.count)
	c.assertContains(t, []string{"debug", "debug 2 will be logged"})

	r.Infoln("info 2 will be logged")
	assert.Equal(t, 4, c.count)
	c.assertContains(t, []string{"info", "info 2 will be logged"})

	r.Errorln("error 2 will be discarded")
	assert.Equal(t, 4, c.count)
	c.assertNotContains(t, "error 2 will be discarded")

	r.Warnln("warning 2 will be discarded")
	assert.Equal(t, 4, c.count)
	c.assertNotContains(t, "warning 2 will be discarded")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP logger_rate_limit_discarded_log_lines_total Total number of discarded log lines per level.
		# TYPE logger_rate_limit_discarded_log_lines_total counter
        logger_rate_limit_discarded_log_lines_total{level="info"} 0
        logger_rate_limit_discarded_log_lines_total{level="debug"} 1
        logger_rate_limit_discarded_log_lines_total{level="error"} 1
        logger_rate_limit_discarded_log_lines_total{level="warning"} 2
	`)))
}

func TestRateLimitedLoggerWithFields(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	c := newCounterLogger(buf)
	reg := prometheus.NewPedanticRegistry()
	logger := NewRateLimitedLogger(c, 0.0001, 1, reg)
	loggerWithFields := logger.WithField("key", "value")

	loggerWithFields.Errorln("Error will be logged")
	assert.Equal(t, 1, c.count)
	c.assertContains(t, []string{"key", "value", "error", "Error will be logged"})

	logger.Infoln("Info will not be logged")
	c.assertNotContains(t, "Info will not be logged")

	loggerWithFields.Debugln("Debug will not be logged")
	c.assertNotContains(t, "Debug will not be logged")

	loggerWithFields.Warnln("Warning will not be logged")
	c.assertNotContains(t, "Warning will not be logged")
	assert.Equal(t, 1, c.count)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP logger_rate_limit_discarded_log_lines_total Total number of discarded log lines per level.
		# TYPE logger_rate_limit_discarded_log_lines_total counter
        logger_rate_limit_discarded_log_lines_total{level="info"} 1
        logger_rate_limit_discarded_log_lines_total{level="debug"} 1
        logger_rate_limit_discarded_log_lines_total{level="warning"} 1
        logger_rate_limit_discarded_log_lines_total{level="error"} 0
	`)))
}

type counterLogger struct {
	logger Interface
	buf    *bytes.Buffer
	count  int
}

func (c *counterLogger) Debugf(format string, args ...interface{}) {
	c.logger.Debugf(format, args...)
	c.count++
}

func (c *counterLogger) Debugln(args ...interface{}) {
	c.logger.Debugln(args...)
	c.count++
}

func (c *counterLogger) Infof(format string, args ...interface{}) {
	c.logger.Infof(format, args...)
	c.count++
}

func (c *counterLogger) Infoln(args ...interface{}) {
	c.logger.Infoln(args...)
	c.count++
}

func (c *counterLogger) Warnf(format string, args ...interface{}) {
	c.logger.Warnf(format, args...)
	c.count++
}

func (c *counterLogger) Warnln(args ...interface{}) {
	c.logger.Warnln(args...)
	c.count++
}

func (c *counterLogger) Errorf(format string, args ...interface{}) {
	c.logger.Errorf(format, args...)
	c.count++
}

func (c *counterLogger) Errorln(args ...interface{}) {
	c.logger.Errorln(args...)
	c.count++
}

func (c *counterLogger) WithField(key string, value interface{}) Interface {
	c.logger = c.logger.WithField(key, value)
	return c
}

func (c *counterLogger) WithFields(fields Fields) Interface {
	c.logger = c.logger.WithFields(fields)
	return c
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
	logrusLogger := logrus.New()
	logrusLogger.Out = buf
	logrusLogger.Level = logrus.DebugLevel
	return &counterLogger{
		logger: Logrus(logrusLogger),
		buf:    buf,
	}
}
