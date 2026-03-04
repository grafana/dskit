// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/logging/level_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package log

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestIsDebugDisabled(t *testing.T) {
	for _, tc := range []struct {
		level string
		want  bool
	}{
		{"debug", false},
		{"info", true},
		{"warn", true},
		{"error", true},
	} {
		t.Run(tc.level, func(t *testing.T) {
			var l Level
			require.NoError(t, l.Set(tc.level))
			require.Equal(t, tc.want, l.IsDebugDisabled())
		})
	}
}

func TestMarshalYAML(t *testing.T) {
	var l Level
	err := l.Set("debug")
	require.NoError(t, err)

	// Test the non-pointed to Level, as people might embed it.
	y, err := yaml.Marshal(l)
	require.NoError(t, err)
	require.Equal(t, []byte("debug\n"), y)

	// And the pointed to Level.
	y, err = yaml.Marshal(&l)
	require.NoError(t, err)
	require.Equal(t, []byte("debug\n"), y)
}
