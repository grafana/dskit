// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/logging/gokit.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package log

import (
	"fmt"
	"io"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

const (
	LogfmtFormat = "logfmt"
)

type Fields []interface{}

func NewFields(fields ...interface{}) Fields {
	return fields
}

func (f Fields) empty() bool {
	return len(f) == 0
}

// NewGoKit creates a new GoKit logger with the given level, format and writer,
// enriched with the standard "ts" and "caller" fields.
// If the given writer is nil, os.Stderr is used.
// If the given format is nil, logfmt is used.
func NewGoKit(lvl Level, format string, writer io.Writer) log.Logger {
	logger := newGoKit(format, writer, nil)
	logger = addStandardFields(logger)
	return level.NewFilter(logger, lvl.Option)
}

// NewGoKitLogFmt creates a new GoKit logger with the given level and writer,
// enriched with the standard "ts" and "caller" fields.
// The "logfmt" format is used.
// If the given writer is nil, os.Stderr is used.
func NewGoKitLogFmt(l Level, writer io.Writer) log.Logger {
	return NewGoKit(l, "logfmt", writer)
}

// NewGoKitWithFields creates a new GoKit logger configured with the given level, format, writer and
// rate limit-related configuration, enriched with the given fields.
// If the given format is nil, logfmt is used.
// If the given writer is nil, os.Stderr is used.
// If the given rate limit configuration is nil, no rate limited logger is created.
// If the given fields are empty, no fields are added.
func NewGoKitWithFields(lvl Level, format string, writer io.Writer, rateLimitedCfg *RateLimitedLoggerCfg, fields Fields) log.Logger {
	logger := newGoKit(format, writer, rateLimitedCfg)
	if !fields.empty() {
		logger = log.With(logger, fields...)
	}
	return level.NewFilter(logger, lvl.Option)
}

func newGoKit(format string, writer io.Writer, rateLimitedCfg *RateLimitedLoggerCfg) log.Logger {
	var logger log.Logger
	if writer == nil {
		writer = log.NewSyncWriter(os.Stderr)
	}
	if format == "json" {
		logger = log.NewJSONLogger(writer)
	} else {
		logger = log.NewLogfmtLogger(writer)
	}
	if rateLimitedCfg != nil {
		logger = NewRateLimitedLogger(logger, *rateLimitedCfg)
	}
	return logger
}

// stand-alone for test purposes
func addStandardFields(logger log.Logger) log.Logger {
	fields := NewFields("ts", log.DefaultTimestampUTC, "caller", log.Caller(5))
	return log.With(logger, fields...)
}

type Sprintf struct {
	format string
	args   []interface{}
}

func LazySprintf(format string, args ...interface{}) *Sprintf {
	return &Sprintf{
		format: format,
		args:   args,
	}
}

func (s *Sprintf) String() string {
	return fmt.Sprintf(s.format, s.args...)
}
