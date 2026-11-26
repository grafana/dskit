package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
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

func TestWithShardKey(t *testing.T) {
	ctx := context.Background()

	wrappers := map[string]func(t *testing.T, next Cache) Cache{
		"LRUCache": func(t *testing.T, next Cache) Cache {
			c, err := WrapWithLRUCache(next, "test", prometheus.NewPedanticRegistry(), 10, time.Minute, log.NewNopLogger())
			require.NoError(t, err)
			return c
		},
		"Versioned": func(_ *testing.T, next Cache) Cache {
			return NewVersioned(next, 1, log.NewNopLogger())
		},
		"SnappyCache": func(_ *testing.T, next Cache) Cache {
			return NewSnappy(next, log.NewNopLogger())
		},
		"SpanlessTracingCache": func(_ *testing.T, next Cache) Cache {
			return NewSpanlessTracingCache(next, log.NewNopLogger(), nil)
		},
	}

	for name, wrap := range wrappers {
		t.Run(name+" forwards the shard key of every write method to the cache it wraps", func(t *testing.T) {
			next := &shardKeyRecordingCache{MockCache: NewMockCache()}
			c := wrap(t, next)
			withShardKey := WithShardKey("object-a")

			c.SetAsync("foo", []byte("1"), time.Minute, withShardKey)
			c.SetMultiAsync(map[string][]byte{"bar": []byte("2")}, time.Minute, withShardKey)
			require.NoError(t, c.Set(ctx, "baz", []byte("3"), time.Minute, withShardKey))
			require.NoError(t, c.Add(ctx, "qux", []byte("4"), time.Minute, withShardKey))
			require.NoError(t, c.Delete(ctx, "foo", withShardKey))

			require.Equal(t, map[string]string{
				"SetAsync":      "object-a",
				"SetMultiAsync": "object-a",
				"Set":           "object-a",
				"Add":           "object-a",
				"Delete":        "object-a",
			}, next.shardKeys())
		})
	}
}

// shardKeyRecordingCache is a MockCache that tracks the shard key of each write call.
type shardKeyRecordingCache struct {
	*MockCache

	mtx  sync.Mutex
	keys map[string]string
}

func (c *shardKeyRecordingCache) record(method string, opts []Option) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.keys == nil {
		c.keys = map[string]string{}
	}
	c.keys[method] = newOptions(opts).ShardKey
}

func (c *shardKeyRecordingCache) shardKeys() map[string]string {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	return c.keys
}

func (c *shardKeyRecordingCache) SetAsync(key string, value []byte, ttl time.Duration, opts ...Option) {
	c.record("SetAsync", opts)
	c.MockCache.SetAsync(key, value, ttl, opts...)
}

func (c *shardKeyRecordingCache) SetMultiAsync(data map[string][]byte, ttl time.Duration, opts ...Option) {
	c.record("SetMultiAsync", opts)
	c.MockCache.SetMultiAsync(data, ttl, opts...)
}

func (c *shardKeyRecordingCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration, opts ...Option) error {
	c.record("Set", opts)
	return c.MockCache.Set(ctx, key, value, ttl, opts...)
}

func (c *shardKeyRecordingCache) Add(ctx context.Context, key string, value []byte, ttl time.Duration, opts ...Option) error {
	c.record("Add", opts)
	return c.MockCache.Add(ctx, key, value, ttl, opts...)
}

func (c *shardKeyRecordingCache) Delete(ctx context.Context, key string, opts ...Option) error {
	c.record("Delete", opts)
	return c.MockCache.Delete(ctx, key, opts...)
}
