// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/logging/level_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package log

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

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
