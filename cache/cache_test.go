package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackendConfig_Validate(t *testing.T) {
	t.Run("empty backend", func(t *testing.T) {
		cfg := BackendConfig{}
		memcachedClientConfigDefaultValues(&cfg.Memcached)

		require.NoError(t, cfg.Validate())
	})

	t.Run("invalid backend", func(t *testing.T) {
		cfg := BackendConfig{
			Backend: "invalid",
		}

		require.Error(t, cfg.Validate())
	})

	t.Run("memcached backend valid", func(t *testing.T) {
		cfg := BackendConfig{}
		memcachedClientConfigDefaultValues(&cfg.Memcached)

		cfg.Backend = BackendMemcached
		cfg.Memcached.Addresses = []string{"localhost:11211"}
		cfg.Memcached.MaxAsyncConcurrency = 1

		require.NoError(t, cfg.Validate())
	})

	t.Run("memcached backend invalid", func(t *testing.T) {
		cfg := BackendConfig{}
		memcachedClientConfigDefaultValues(&cfg.Memcached)

		cfg.Backend = BackendMemcached
		cfg.Memcached.Addresses = []string{}

		require.Equal(t, ErrNoMemcachedAddresses, cfg.Validate())
	})
}
