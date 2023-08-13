// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/logging/gokit_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package log

import (
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func BenchmarkDebugf(b *testing.B) {
	lvl := Level{Option: level.AllowInfo()}
	g := log.NewNopLogger()
	logger := addStandardFields(g, lvl)
	// Simulate the parameters used in middleware/logging.go
	var (
		method     = "method"
		uri        = "https://example.com/foobar"
		statusCode = 404
		duration   = 42
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		level.Debug(logger).Log("msg", fmt.Sprintf("%s %s (%d) %v", method, uri, statusCode, duration))
	}
}
