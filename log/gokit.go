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

// NewGoKit creates a new GoKit logger with the given level, format and writer.
// If the given writer is nil, os.Stderr is used.
// If the given format is nil, logfmt is used.
func NewGoKit(lvl Level, format string, writer io.Writer) log.Logger {
	logger := newGoKit(format, writer)
	return level.NewFilter(logger, lvl.Option)
}

func newGoKit(format string, writer io.Writer) log.Logger {
	if writer == nil {
		writer = log.NewSyncWriter(os.Stderr)
	}
	if format == "json" {
		return log.NewJSONLogger(writer)
	}
	return log.NewLogfmtLogger(writer)
}

// NewGoKitWithFields creates a new GoKit logger with the given level and writer,
// enriched with the "ts" and the "caller" fields for test purposes.
// The "logfmt" format is used.
// If the given writer is nil, os.Stderr is used.
func NewGoKitWithFields(l Level, writer io.Writer) log.Logger {
	logger := newGoKit(LogfmtFormat, writer)
	return addStandardFields(logger, l)
}

// stand-alone for test purposes
func addStandardFields(logger log.Logger, l Level) log.Logger {
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.Caller(5))
	return level.NewFilter(logger, l.Option)
}

type lazySprintf interface {
	String() string
}

type gokitLazySprintf struct {
	format  string
	args    []interface{}
	sprintf func(string, ...any) string
}

func NewGokitLazySprintf(format string, args []interface{}) lazySprintf {
	return &gokitLazySprintf{
		format:  format,
		args:    args,
		sprintf: fmt.Sprintf,
	}
}

func (s *gokitLazySprintf) String() string {
	return s.sprintf(s.format, s.args...)
}
