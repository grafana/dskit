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

// NewGoKit creates a new GoKit logger with the given level, format and writer,
// enriched with the standard "ts" and "caller" fields.
// If the given writer is nil, os.Stderr is used.
// If the given format is nil, logfmt is used.
func NewGoKit(lvl Level, format string, writer io.Writer) log.Logger {
	if writer == nil {
		writer = log.NewSyncWriter(os.Stderr)
	}
	if format == "json" {
		return log.NewJSONLogger(writer)
	}
	logger := addStandardFields(log.NewLogfmtLogger(writer))
	return level.NewFilter(logger, lvl.Option)
}

// NewGoKitWithFields creates a new GoKit logger with the given level and writer,
// enriched with the standard "ts" and "caller" fields.
// The "logfmt" format is used.
// If the given writer is nil, os.Stderr is used.
func NewGoKitWithFields(l Level, writer io.Writer) log.Logger {
	return NewGoKit(l, "logfmt", writer)
}

// stand-alone for test purposes
func addStandardFields(logger log.Logger) log.Logger {
	return log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.Caller(5))
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
