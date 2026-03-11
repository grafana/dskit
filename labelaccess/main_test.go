// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"os"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
)

func TestMain(m *testing.M) {
	SetSelectorParser(parser.ParseMetricSelector)
	os.Exit(m.Run())
}
