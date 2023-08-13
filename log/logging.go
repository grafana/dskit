// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/logging/logging.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package log

import (
	"fmt"
	"os"
)

// Setup configures a global Gokit logger to output to stderr.
// It populates the Gokit logger as well as the global logging instance.
func Setup(logLevel string) error {
	var level Level
	err := level.Set(logLevel)
	if err != nil {
		return fmt.Errorf("error parsing log level: %v", err)
	}

	// TODO understand whether this is needed
	/*hook, err := promrus.NewPrometheusHook() // Expose number of log messages as Prometheus metrics.
	if err != nil {
		return err
	}*/

	logger := NewGoKitWriter(level, os.Stderr)
	SetGlobal(logger)
	return nil
}
