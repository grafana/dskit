// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	BackendMemcached = "memcached"
)

var (
	ErrNotStored    = errors.New("item not stored")
	ErrInvalidTTL   = errors.New("invalid TTL")
	ErrCASConflict  = errors.New("compare-and-swap conflict")
	ErrItemTooLarge = errors.New("item too large")
)

// Cache is a high level interface to interact with a cache.
type Cache interface {
	// GetMulti fetches multiple keys at once from a cache. In case of error,
	// an empty map is returned and the error tracked/logged. One or more Option
	// instances may be passed to modify the behavior of this GetMulti call.
	GetMulti(ctx context.Context, keys []string, opts ...Option) map[string][]byte

	// GetMultiWithError fetches multiple keys at once from a cache and returns an error
	// if the operation failed. It may return both results and an error if there are only partial
	// failures. One or more Option instances may be passed to modify the
	// behavior of this GetMultiWithError call.
	GetMultiWithError(ctx context.Context, keys []string, opts ...Option) (map[string][]byte, error)

	// SetAsync enqueues an operation to store a key into a cache. In case the underlying
	// operation fails, the error will be tracked/logged. Pass an Option to modify the
	// behavior of this SetAsync call.
	SetAsync(key string, value []byte, ttl time.Duration, opts ...Option)

	// SetMultiAsync enqueues operations to store a keys and values into a cache. In case
	// any underlying async operations fail, the errors will be tracked/logged. Pass an
	// Option to modify the behavior of this SetMultiAsync call. It applies to every key.
	SetMultiAsync(data map[string][]byte, ttl time.Duration, opts ...Option)

	// Set stores a key and value into a cache. Pass an Option to modify the behavior of
	// this Set call.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration, opts ...Option) error

	// Add stores a key and value into a cache only if it does not already exist. If the
	// item was not stored because an entry already exists in the cache, ErrNotStored will
	// be returned. Pass an Option to modify the behavior of this Add call.
	Add(ctx context.Context, key string, value []byte, ttl time.Duration, opts ...Option) error

	// Delete deletes a key from a cache. Pass an Option to modify the behavior of this
	// Delete call.
	Delete(ctx context.Context, key string, opts ...Option) error

	// Stop client and release underlying resources.
	Stop()

	// Name returns the name of this particular cache instance.
	Name() string
}

// Options are used to modify the behavior of an individual call to a cache
// backend. They are constructed by applying Option callbacks passed to a client
// method to a default Options instance.
type Options struct {
	Alloc Allocator

	// ShardKey, when it is not empty, picks the cache server of the call instead
	// of the cache key.
	ShardKey string
}

// Option is a callback used to modify the Options that a particular client
// method uses.
type Option func(opts *Options)

// newOptions returns the Options that opts build.
func newOptions(opts []Option) *Options {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// WithAllocator creates a new Option that makes use of a specific memory Allocator
// for cache result values.
func WithAllocator(alloc Allocator) Option {
	return func(opts *Options) {
		opts.Alloc = alloc
	}
}

// WithShardKey creates a new Option that picks the cache server of a call from
// shardKey, instead of from the cache key. Items that use the same shard key sit
// on the same server, so a GetMulti for them sends one request to one server.
// An empty shardKey means no shard key. If a call has several WithShardKey
// options, the last one wins.
//
// A call must use the shard key that stored the item. Without it, the call picks
// the server of the cache key, which can be another server. A read then misses
// the item, and a write stores a second copy. The cache key alone identifies an
// item on a server, so keep cache keys unique across shard keys.
//
// The caller owns the choice of the shard key. The cache does not check that the
// items of a shard key fit on one server, or that one server can carry their
// load. A shard key that groups too many items can fill or overload its server.
//
// At the moment, only the MemcachedClient supports the shard key. A cache that
// does not support it ignores it, and a cache that wraps another cache passes it
// on.
func WithShardKey(shardKey string) Option {
	return func(opts *Options) {
		opts.ShardKey = shardKey
	}
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

type BackendConfig struct {
	Backend   string                `yaml:"backend"`
	Memcached MemcachedClientConfig `yaml:"memcached"`
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
		return NewMemcachedClientWithConfig(logger, cacheName, cfg.Memcached, reg)
	default:
		return nil, errors.Errorf("unsupported cache type for cache %s: %s", cacheName, cfg.Backend)
	}
}
