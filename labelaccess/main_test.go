// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"os"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
)

func TestMain(m *testing.M) {
	// This is needed to initiate ParseMetricSelector for tests that might use it.
	SetSelectorParser(parser.ParseMetricSelector)
	os.Exit(m.Run())
}
