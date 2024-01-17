package cache

import (
	"context"
	"flag"
	"fmt"
	"hash/crc32"
	"net"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
)

func TestMemcachedClientConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup       func(config *MemcachedClientConfig)
		expectedErr error
	}{
		"should pass on positive write buffer size": {
			setup: func(config *MemcachedClientConfig) {
				config.WriteBufferSizeBytes = 12345
			},
			expectedErr: nil,
		},
		"should pass on positive read buffer size": {
			setup: func(config *MemcachedClientConfig) {
				config.ReadBufferSizeBytes = 12345
			},
			expectedErr: nil,
		},
		"should return error on negative write buffer size": {
			setup: func(config *MemcachedClientConfig) {
				config.WriteBufferSizeBytes = -1
			},
			expectedErr: ErrInvalidWriteBufferSizeBytes,
		},
		"should return error on negative read buffer size": {
			setup: func(config *MemcachedClientConfig) {
				config.ReadBufferSizeBytes = -1
			},
			expectedErr: ErrInvalidReadBufferSizeBytes,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := MemcachedClientConfig{}
			memcachedClientConfigDefaultValues(&cfg)
			cfg.Addresses = flagext.StringSliceCSV{"localhost:11211"}

			testData.setup(&cfg)
			require.Equal(t, testData.expectedErr, cfg.Validate())
		})
	}
}

func TestMemcachedClient_GetMulti(t *testing.T) {
	setup := func() (*MemcachedClient, *mockMemcachedClientBackend, error) {
		backend := newMockMemcachedClientBackend()
		client, err := newMemcachedClient(
			log.NewNopLogger(),
			backend,
			&mockServerSelector{},
			MemcachedClientConfig{
				Addresses:           []string{"localhost"},
				MaxAsyncConcurrency: 1,
				MaxAsyncBufferSize:  10,
			},
			prometheus.NewPedanticRegistry(),
			"test",
		)

		return client, backend, err
	}

	t.Run("no allocator", func(t *testing.T) {
		client, backend, err := setup()
		require.NoError(t, err)
		client.SetAsync("foo", []byte("bar"), 10*time.Second)
		require.NoError(t, client.wait())

		ctx := context.Background()
		res := client.GetMulti(ctx, []string{"foo"})
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, res)
		require.Equal(t, 0, backend.allocations)
	})

	t.Run("with allocator", func(t *testing.T) {
		client, backend, err := setup()
		require.NoError(t, err)
		client.SetAsync("foo", []byte("bar"), 10*time.Second)
		require.NoError(t, client.wait())

		res := client.GetMulti(context.Background(), []string{"foo"}, WithAllocator(&nopAllocator{}))
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, res)
		require.Equal(t, 1, backend.allocations)
	})
}

func BenchmarkMemcachedClient_sortKeysByServer(b *testing.B) {
	mockSelector := &mockServerSelector{
		servers: []mockServer{
			{addr: "127.0.0.1"},
			{addr: "127.0.0.2"},
			{addr: "127.0.0.3"},
			{addr: "127.0.0.4"},
			{addr: "127.0.0.5"},
			{addr: "127.0.0.6"},
			{addr: "127.0.0.7"},
			{addr: "127.0.0.8"},
		},
	}

	client, err := newMemcachedClient(
		log.NewNopLogger(),
		newMockMemcachedClientBackend(),
		mockSelector,
		MemcachedClientConfig{
			Addresses:           []string{"localhost"},
			MaxAsyncConcurrency: 1,
			MaxAsyncBufferSize:  10,
		},
		prometheus.NewPedanticRegistry(),
		"test",
	)

	if err != nil {
		b.Fatal("unexpected error creating MemcachedClient", err)
	}

	const numKeys = 10_000

	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("some-key:%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.sortKeysByServer(keys)
	}
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

func (m *mockMemcachedClientBackend) Close() {}

type mockServer struct {
	addr string
}

func (m mockServer) Network() string {
	return "tcp"
}

func (m mockServer) String() string {
	return m.addr
}

type mockServerSelector struct {
	servers []mockServer
}

func (s *mockServerSelector) PickServer(key string) (net.Addr, error) {
	cs := crc32.ChecksumIEEE([]byte(key))
	return s.servers[cs%uint32(len(s.servers))], nil
}
func (s *mockServerSelector) Each(f func(net.Addr) error) error {
	for _, srv := range s.servers {
		if err := f(srv); err != nil {
			return err
		}
	}

	return nil
}
func (s *mockServerSelector) SetServers(_ ...string) error {
	return nil
}

type nopAllocator struct{}

func (n nopAllocator) Get(_ int) *[]byte { return nil }
func (n nopAllocator) Put(_ *[]byte)     {}

func memcachedClientConfigDefaultValues(cfg *MemcachedClientConfig) {
	// Init default values. We can't use flagext.DefaultValues() because MemcachedClientConfig
	// does not implement any of the supported interfaces.
	fs := flag.NewFlagSet("", flag.PanicOnError)
	cfg.RegisterFlagsWithPrefix("", fs)
	_ = fs.Parse([]string{})
}
