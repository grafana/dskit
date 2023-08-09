package log

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type counterLogger struct {
	count int
}

func (c *counterLogger) Debugf(_ string, _ ...interface{}) { c.count++ }
func (c *counterLogger) Debugln(_ ...interface{})          { c.count++ }
func (c *counterLogger) Infof(_ string, _ ...interface{})  { c.count++ }
func (c *counterLogger) Infoln(_ ...interface{})           { c.count++ }
func (c *counterLogger) Warnf(_ string, _ ...interface{})  { c.count++ }
func (c *counterLogger) Warnln(_ ...interface{})           { c.count++ }
func (c *counterLogger) Errorf(_ string, _ ...interface{}) { c.count++ }
func (c *counterLogger) Errorln(_ ...interface{})          { c.count++ }
func (c *counterLogger) WithField(_ string, _ interface{}) Interface {
	return c
}
func (c *counterLogger) WithFields(Fields) Interface {
	return c
}

func TestRateLimitedLoggerLogs(t *testing.T) {
	c := &counterLogger{}
	r := NewRateLimitedLogger(c, 1, 1)

	r.Errorln("asdf")
	assert.Equal(t, 1, c.count)
}

func TestRateLimitedLoggerLimits(t *testing.T) {
	c := &counterLogger{}
	r := NewRateLimitedLogger(c, 2, 2)

	r.Errorln("asdf")
	r.Infoln("asdf")
	r.Debugln("asdf")
	assert.Equal(t, 2, c.count)
	time.Sleep(time.Second)
	r.Infoln("asdf")
	assert.Equal(t, 3, c.count)
}

func TestRateLimitedLoggerWithFields(t *testing.T) {
	c := &counterLogger{}
	r := NewRateLimitedLogger(c, 1, 1)
	r2 := r.WithField("key", "value")

	r.Errorf("asdf")
	r2.Errorln("asdf")
	r2.Warnln("asdf")
	assert.Equal(t, 1, c.count)
}
