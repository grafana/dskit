package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackendConfig_Validate(t *testing.T) {
	t.Run("empty backend", func(t *testing.T) {
		cfg := BackendConfig{}

		require.NoError(t, cfg.Validate())
	})

	t.Run("invalid backend", func(t *testing.T) {
		cfg := BackendConfig{
			Backend: "invalid",
		}

		require.Error(t, cfg.Validate())
	})

	t.Run("memcached backend valid", func(t *testing.T) {
		cfg := BackendConfig{
			Backend: BackendMemcached,
			Memcached: MemcachedClientConfig{
				Addresses:           []string{"localhost:11211"},
				MaxAsyncConcurrency: 1,
			},
		}

		require.NoError(t, cfg.Validate())
	})

	t.Run("memcached backend invalid", func(t *testing.T) {
		cfg := BackendConfig{
			Backend:   BackendMemcached,
			Memcached: MemcachedClientConfig{},
		}

		require.Error(t, cfg.Validate())
	})

	t.Run("redis backend valid", func(t *testing.T) {
		cfg := BackendConfig{
			Backend: BackendRedis,
			Redis: RedisClientConfig{
				Endpoint:            []string{"localhost:6379"},
				MaxAsyncConcurrency: 1,
			},
		}

		require.NoError(t, cfg.Validate())
	})

	t.Run("redis backend invalid", func(t *testing.T) {
		cfg := BackendConfig{
			Backend: BackendRedis,
			Redis:   RedisClientConfig{},
		}

		require.Error(t, cfg.Validate())
	})
}
