package kv

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func boolPtr(b bool) *bool {
	return &b
}

func TestMultiRuntimeConfigWithVariousEnabledValues(t *testing.T) {
	testcases := map[string]struct {
		yaml     string
		expected *bool
	}{
		"nil":   {"primary: test", nil},
		"true":  {"primary: test\nmirror_enabled: true", boolPtr(true)},
		"false": {"mirror_enabled: false", boolPtr(false)},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			c := MultiRuntimeConfig{}
			err := yaml.Unmarshal([]byte(tc.yaml), &c)
			assert.NoError(t, err, tc.yaml)
			assert.Equal(t, tc.expected, c.Mirroring, tc.yaml)
		})
	}
}

// This simple test could reproduce panic in watchConfigChannel goroutine, if metrics didn't exist yet.
func TestNewMulti(t *testing.T) {
	cfg := MultiConfig{ConfigProvider: func() <-chan MultiRuntimeConfig {
		ch := make(chan MultiRuntimeConfig, 1)
		ch <- MultiRuntimeConfig{
			PrimaryStore: "b",
		}
		return ch
	}}

	clients := []kvclient{{name: "a"}, {name: "b"}}

	mc := NewMultiClient(cfg, clients, log.NewNopLogger(), nil)
	t.Log("canceling MultiClient")
	mc.cancel()
}
