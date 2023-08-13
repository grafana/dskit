// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/logging/gokit.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package log

import (
	"io"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// NewGoKitFormat creates a new log.Logger whose format can be "json" or "logfmt".
func NewGoKitFormat(l Level, format string) log.Logger {
	var logger log.Logger
	if format == "json" {
		logger = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
	} else {
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	}
	return addStandardFields(logger, l)
}

// stand-alone for test purposes
func addStandardFields(logger log.Logger, l Level) log.Logger {
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.Caller(5))
	return level.NewFilter(logger, l.Option)
}

// NewGoKit creates a new GoKit logger with the "logfmt" format.
func NewGoKit(l Level) log.Logger {
	return NewGoKitFormat(l, "logfmt")
}

// NewGoKitWriter creates a new GoKit logger with the passed level and writer.
func NewGoKitWriter(l Level, writer io.Writer) log.Logger {
	logger := log.NewLogfmtLogger(writer)
	return addStandardFields(logger, l)
}
