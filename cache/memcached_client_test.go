package cache

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMemcachedClient_GetMulti(t *testing.T) {
	setup := func() (*memcachedClient, *mockMemcachedClientBackend, error) {
		backend := newMockMemcachedClientBackend()
		client, err := newMemcachedClient(
			log.NewNopLogger(),
			backend,
			&mockServerSelector{},
			MemcachedClientConfig{
				Addresses:                 []string{"localhost"},
				MaxAsyncConcurrency:       1,
				MaxAsyncBufferSize:        10,
				DNSProviderUpdateInterval: 10 * time.Second,
			},
			prometheus.NewPedanticRegistry(),
			"test",
		)

		return client, backend, err
	}

	t.Run("no allocator", func(t *testing.T) {
		client, backend, err := setup()
		require.NoError(t, err)
		require.NoError(t, client.SetAsync(context.Background(), "foo", []byte("bar"), 10*time.Second))
		require.NoError(t, client.wait())

		ctx := context.Background()
		res := client.GetMulti(ctx, []string{"foo"})
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, res)
		require.Equal(t, 0, backend.allocations)
	})

	t.Run("with allocator", func(t *testing.T) {
		client, backend, err := setup()
		require.NoError(t, err)
		require.NoError(t, client.SetAsync(context.Background(), "foo", []byte("bar"), 10*time.Second))
		require.NoError(t, client.wait())

		res := client.GetMulti(context.Background(), []string{"foo"}, WithAllocator(&nopAllocator{}))
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, res)
		require.Equal(t, 1, backend.allocations)
	})
}

type mockMemcachedClientBackend struct {
	allocations int
	values      map[string]*memcache.Item
}

func newMockMemcachedClientBackend() *mockMemcachedClientBackend {
	return &mockMemcachedClientBackend{
		values: make(map[string]*memcache.Item),
	}
}

func (m *mockMemcachedClientBackend) GetMulti(keys []string, opts ...memcache.Option) (map[string]*memcache.Item, error) {
	options := &memcache.Options{}
	for _, opt := range opts {
		opt(options)
	}

	if options.Alloc != nil {
		m.allocations++
	}

	out := make(map[string]*memcache.Item)
	for _, k := range keys {
		if v, ok := m.values[k]; ok {
			out[k] = v
		}
	}

	return out, nil
}

func (m *mockMemcachedClientBackend) Set(item *memcache.Item) error {
	m.values[item.Key] = item
	return nil
}

func (m *mockMemcachedClientBackend) Delete(key string) error {
	delete(m.values, key)
	return nil
}

type mockServerSelector struct{}

func (m mockServerSelector) SetServers(_ ...string) error {
	return nil
}

func (m mockServerSelector) PickServer(key string) (net.Addr, error) {
	return nil, errors.New("mock server selector")
}

func (m mockServerSelector) Each(f func(net.Addr) error) error {
	return nil
}

type nopAllocator struct{}

func (n nopAllocator) Get(_ int) *[]byte { return nil }
func (n nopAllocator) Put(_ *[]byte)     {}
