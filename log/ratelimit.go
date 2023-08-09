package log

import "golang.org/x/time/rate"

type rateLimitedLogger struct {
	next    Interface
	limiter *rate.Limiter
}

// NewRateLimitedLogger returns a logger.Interface that is limited to the given number of logs per second,
// with the given burst size.
func NewRateLimitedLogger(logger Interface, logsPerSecond rate.Limit, burstSize int) Interface {
	return &rateLimitedLogger{
		next:    logger,
		limiter: rate.NewLimiter(logsPerSecond, burstSize),
	}
}

func (l *rateLimitedLogger) Debugf(format string, args ...interface{}) {
	if l.limiter.Allow() {
		l.next.Debugf(format, args...)
	}
}

func (l *rateLimitedLogger) Debugln(args ...interface{}) {
	if l.limiter.Allow() {
		l.next.Debugln(args...)
	}
}

func (l *rateLimitedLogger) Infof(format string, args ...interface{}) {
	if l.limiter.Allow() {
		l.next.Infof(format, args...)
	}
}

func (l *rateLimitedLogger) Infoln(args ...interface{}) {
	if l.limiter.Allow() {
		l.next.Infoln(args...)
	}
}

func (l *rateLimitedLogger) Errorf(format string, args ...interface{}) {
	if l.limiter.Allow() {
		l.next.Errorf(format, args...)
	}
}

func (l *rateLimitedLogger) Errorln(args ...interface{}) {
	if l.limiter.Allow() {
		l.next.Errorln(args...)
	}
}

func (l *rateLimitedLogger) Warnf(format string, args ...interface{}) {
	if l.limiter.Allow() {
		l.next.Warnf(format, args...)
	}
}

func (l *rateLimitedLogger) Warnln(args ...interface{}) {
	if l.limiter.Allow() {
		l.next.Warnln(args...)
	}
}

func (l *rateLimitedLogger) WithField(key string, value interface{}) Interface {
	return &rateLimitedLogger{
		next:    l.next.WithField(key, value),
		limiter: l.limiter,
	}
}

func (l *rateLimitedLogger) WithFields(f Fields) Interface {
	return &rateLimitedLogger{
		next:    l.next.WithFields(f),
		limiter: l.limiter,
	}
}
