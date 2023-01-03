package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type contextKey int

const (
	contextKeyAllocator contextKey = 0
)

// RemoteCacheClient is a high level client to interact with remote cache.
type RemoteCacheClient interface {
	// GetMulti fetches multiple keys at once from remoteCache. In case of error,
	// an empty map is returned and the error tracked/logged.
	GetMulti(ctx context.Context, keys []string) map[string][]byte

	// SetAsync enqueues an asynchronous operation to store a key into memcached.
	// Returns an error in case it fails to enqueue the operation. In case the
	// underlying async operation will fail, the error will be tracked/logged.
	SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Delete deletes a key from memcached.
	// This is a synchronous operation. If an asynchronous set operation for key is still
	// pending to be processed, it will wait for it to complete before performing deletion.
	Delete(ctx context.Context, key string) error

	// Stop client and release underlying resources.
	Stop()
}

// Cache is a generic interface.
type Cache interface {
	// Store data into the cache.
	//
	// Note that individual byte buffers may be retained by the cache!
	Store(ctx context.Context, data map[string][]byte, ttl time.Duration)

	// Fetch multiple keys from cache. Returns map of input keys to data.
	// If key isn't in the map, data for given key was not found.
	Fetch(ctx context.Context, keys []string) map[string][]byte

	Name() string
}

// WithAllocator returns a Context that makes use of a specific memory Allocator
// for result values loaded by a cache.
func WithAllocator(ctx context.Context, alloc Allocator) context.Context {
	return context.WithValue(ctx, contextKeyAllocator, alloc)
}

// GetAllocator returns the Allocator set for this particular context, if any.
func GetAllocator(ctx context.Context) Allocator {
	val := ctx.Value(contextKeyAllocator)
	if val != nil {
		return val.(Allocator)
	}

	return nil
}

// Allocator allows memory for cache result values to be managed by callers instead of by
// a cache client itself. For example, this can be used by callers to implement arena-style
// memory management if a workload tends to be request-centric.
type Allocator interface {
	// Get returns a byte slice with at least sz capacity. Length of the slice is
	// not guaranteed and so must be asserted by callers (cache clients).
	Get(sz int) *[]byte
	// Put returns the byte slice to the underlying allocator. The cache clients
	// will only call this method during error handling when allocated values are
	// not returned to the caller as cache results.
	Put(b *[]byte)
}

const (
	BackendMemcached = "memcached"
	BackendRedis     = "redis"
)

type BackendConfig struct {
	Backend   string          `yaml:"backend"`
	Memcached MemcachedConfig `yaml:"memcached"`
}

// Validate the config.
func (cfg *BackendConfig) Validate() error {
	if cfg.Backend != "" && cfg.Backend != BackendMemcached {
		return fmt.Errorf("unsupported cache backend: %s", cfg.Backend)
	}

	switch cfg.Backend {
	case BackendMemcached:
		return cfg.Memcached.Validate()
	}
	return nil
}

func CreateClient(cacheName string, cfg BackendConfig, logger log.Logger, reg prometheus.Registerer) (Cache, error) {
	switch cfg.Backend {
	case "":
		// No caching.
		return nil, nil

	case BackendMemcached:
		client, err := NewMemcachedClientWithConfig(logger, cacheName, cfg.Memcached.ToMemcachedClientConfig(), reg)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create memcached client")
		}
		return NewMemcachedCache(cacheName, logger, client, reg), nil

	default:
		return nil, errors.Errorf("unsupported cache type for cache %s: %s", cacheName, cfg.Backend)
	}
}
