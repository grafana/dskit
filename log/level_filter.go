package log

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// FilteredLogger wraps a go-kit/log level-filter logger and exposes a
// DebugEnabled() method so that callers can skip building expensive log-field
// chains when debug output will be suppressed by the filter.
//
// Use NewFilteredLogger (or NewGoKitWithLevel) in place of level.NewFilter
// when you need the DebugEnabled optimisation.
type FilteredLogger struct {
	log.Logger        // the underlying level-filtered logger
	debugEnabled bool // whether the configured level allows debug messages
}

// NewFilteredLogger returns a FilteredLogger that wraps next with a level
// filter configured by lvl.  It satisfies the middleware.DebugEnabled interface
// so that interceptors can skip building log-field chains when debug output is
// suppressed.
func NewFilteredLogger(next log.Logger, lvl Level) *FilteredLogger {
	return &FilteredLogger{
		Logger:       level.NewFilter(next, lvl.Option),
		debugEnabled: lvl.s == "debug",
	}
}

// DebugEnabled reports whether this logger will emit debug-level messages.
func (f *FilteredLogger) DebugEnabled() bool {
	return f.debugEnabled
}
