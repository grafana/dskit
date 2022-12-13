package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemcachedIndexCacheConfig_GetAddresses(t *testing.T) {
	tests := map[string]struct {
		cfg      MemcachedConfig
		expected []string
	}{
		"no addresses": {
			cfg: MemcachedConfig{
				Addresses: "",
			},
			expected: []string{},
		},
		"one address": {
			cfg: MemcachedConfig{
				Addresses: "dns+localhost:11211",
			},
			expected: []string{"dns+localhost:11211"},
		},
		"two addresses": {
			cfg: MemcachedConfig{
				Addresses: "dns+memcached-1:11211,dns+memcached-2:11211",
			},
			expected: []string{"dns+memcached-1:11211", "dns+memcached-2:11211"},
		},
	}
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.GetAddresses())
		})
	}
}
