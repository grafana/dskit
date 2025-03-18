package cache

import (
	"context"
	"flag"
	"fmt"
	"hash/crc32"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
)

func TestMemcachedClient_SetAsync(t *testing.T) {
	t.Run("with non-zero TTL", func(t *testing.T) {
		client, _, err := setupDefaultMemcachedClient()
		require.NoError(t, err)
		client.SetAsync("foo", []byte("bar"), 10*time.Second)
		require.NoError(t, client.wait())

		ctx := context.Background()
		res := client.GetMulti(ctx, []string{"foo"})
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, res)
	})

	t.Run("with truncated zero TTL", func(t *testing.T) {
		client, _, err := setupDefaultMemcachedClient()
		require.NoError(t, err)
		client.SetAsync("foo", []byte("bar"), 100*time.Millisecond)
		require.NoError(t, client.wait())

		ctx := context.Background()
		res := client.GetMulti(ctx, []string{"foo"})
		require.Empty(t, res)
	})

	t.Run("with zero TTL", func(t *testing.T) {
		client, _, err := setupDefaultMemcachedClient()
		require.NoError(t, err)
		client.SetAsync("foo", []byte("bar"), 0)
		require.NoError(t, client.wait())

		ctx := context.Background()
		res := client.GetMulti(ctx, []string{"foo"})
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, res)
	})
}

func TestMemcachedClient_Set(t *testing.T) {
	t.Run("with non-zero TTL", func(t *testing.T) {
		ctx := context.Background()
		client, _, err := setupDefaultMemcachedClient()
		require.NoError(t, err)
		require.NoError(t, client.Set(ctx, "foo", []byte("bar"), time.Minute))

		res := client.GetMulti(ctx, []string{"foo"})
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, res)
	})

	t.Run("with truncated TTL", func(t *testing.T) {
		ctx := context.Background()
		client, _, err := setupDefaultMemcachedClient()
		require.NoError(t, err)
		err = client.Set(ctx, "foo", []byte("bar"), 100*time.Millisecond)
		require.ErrorIs(t, err, ErrInvalidTTL)
	})

	t.Run("with zero TTL", func(t *testing.T) {
		ctx := context.Background()
		client, _, err := setupDefaultMemcachedClient()
		require.NoError(t, err)
		require.NoError(t, client.Set(ctx, "foo", []byte("bar"), 0))

		res := client.GetMulti(ctx, []string{"foo"})
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, res)
	})
}

func TestMemcachedClient_Add(t *testing.T) {
	t.Run("item does not exist", func(t *testing.T) {
		ctx := context.Background()
		client, _, err := setupDefaultMemcachedClient()
		require.NoError(t, err)
		require.NoError(t, client.Add(ctx, "foo", []byte("bar"), time.Minute))
	})

	t.Run("item already exists", func(t *testing.T) {
		ctx := context.Background()
		client, mock, err := setupDefaultMemcachedClient()
		require.NoError(t, err)

		require.NoError(t, mock.Set(&memcache.Item{
			Key:        "foo",
			Value:      []byte("baz"),
			Expiration: 60,
		}))

		err = client.Add(ctx, "foo", []byte("bar"), time.Minute)
		require.ErrorIs(t, err, ErrNotStored)
	})
}

func TestMemcachedClient_GetMulti(t *testing.T) {
	t.Run("no allocator", func(t *testing.T) {
		client, backend, err := setupDefaultMemcachedClient()
		require.NoError(t, err)
		client.SetAsync("foo", []byte("bar"), 10*time.Second)
		require.NoError(t, client.wait())

		ctx := context.Background()
		res := client.GetMulti(ctx, []string{"foo"})
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, res)
		require.Equal(t, 0, backend.allocations)
	})

	t.Run("with allocator", func(t *testing.T) {
		client, backend, err := setupDefaultMemcachedClient()
		require.NoError(t, err)
		client.SetAsync("foo", []byte("bar"), 10*time.Second)
		require.NoError(t, client.wait())

		res := client.GetMulti(context.Background(), []string{"foo"}, WithAllocator(&nopAllocator{}))
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, res)
		require.Equal(t, 1, backend.allocations)
	})
}

func TestMemcachedClient_Increment(t *testing.T) {
	client, _, err := setupDefaultMemcachedClient()
	require.NoError(t, err)

	key := "foo"
	initialValue := []byte("2")

	client.SetAsync(key, initialValue, 10*time.Second)
	require.NoError(t, client.wait())

	incrementedValue, err := client.Increment(context.Background(), key, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(3), incrementedValue)
}

func TestMemcachedClient_Decrement(t *testing.T) {
	client, _, err := setupDefaultMemcachedClient()
	require.NoError(t, err)

	key := "foo"
	initialValue := []byte("2")

	client.SetAsync(key, initialValue, 10*time.Second)
	require.NoError(t, client.wait())

	incrementedValue, err := client.Decrement(context.Background(), key, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), incrementedValue)
}

func TestMemcachedClient_Touch(t *testing.T) {
	client, backend, err := setupDefaultMemcachedClient()
	require.NoError(t, err)

	key := "foo"
	value := []byte("bar")
	expiration := 10 * time.Second

	client.SetAsync(key, value, expiration)
	require.NoError(t, client.wait())

	newExpiration := 20 * time.Second
	err = client.Touch(context.Background(), key, newExpiration)
	require.NoError(t, err)
	require.Equal(t, int32(newExpiration.Seconds()), backend.values[key].Expiration)
}

func TestMemcachedClient_CompareAndSwap(t *testing.T) {
	client, backend, err := setupDefaultMemcachedClient()
	require.NoError(t, err)

	key := "foo"
	initialValue := []byte("bar")
	expiration := 10 * time.Second

	client.SetAsync(key, initialValue, expiration)
	require.NoError(t, client.wait())

	newValue := []byte("baz")
	newExpiration := 20 * time.Second
	err = client.CompareAndSwap(context.Background(), key, newValue, newExpiration)
	require.NoError(t, err)

	storedItem := backend.values[key]
	require.Equal(t, newValue, storedItem.Value)
	require.Equal(t, int32(newExpiration.Seconds()), storedItem.Expiration)
}

func TestMemcachedClient_FlushAll(t *testing.T) {
	client, _, err := setupDefaultMemcachedClient()
	require.NoError(t, err)

	keys := []string{"foo", "bar"}
	expiration := 10 * time.Second

	client.SetMultiAsync(map[string][]byte{"foo": []byte("bar"), "bar": []byte("baz")}, expiration)
	require.NoError(t, client.wait())

	// Verify that the data is initially present
	tmpRes := client.GetMulti(context.Background(), keys)
	require.NotEmpty(t, tmpRes)

	// FlushAll operation
	err = client.FlushAll(context.Background())
	require.NoError(t, err)

	// Verify that the cash is empty after FlushAll
	res := client.GetMulti(context.Background(), keys)
	require.Empty(t, res)
}

func TestMemcachedClient_Delete(t *testing.T) {
	client, _, err := setupDefaultMemcachedClient()
	require.NoError(t, err)

	key := "foo"
	value := []byte("bar")
	expiration := 10 * time.Second

	client.SetAsync(key, value, expiration)
	require.NoError(t, client.wait())

	// Verify that the key is initially present
	initialResult := client.GetMulti(context.Background(), []string{key})
	require.Equal(t, map[string][]byte{key: value}, initialResult)

	// Delete operation
	err = client.Delete(context.Background(), key)
	require.NoError(t, err)

	// Verify that the key is no longer present after deletion
	afterDeletionResult := client.GetMulti(context.Background(), []string{key})
	require.Empty(t, afterDeletionResult)
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

func TestMemcachedClient_ServerDependency(t *testing.T) {
	testCases := []struct {
		name                     string
		dnsIgnoreStartupFailures bool
	}{
		{
			name:                     "with DNS failures not ignored",
			dnsIgnoreStartupFailures: false,
		},
		{
			name:                     "with DNS failures ignored",
			dnsIgnoreStartupFailures: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &MemcachedClientConfig{}
			memcachedClientConfigDefaultValues(cfg)
			cfg.Addresses = flagext.StringSliceCSV{"dns+invalid.:11211"}
			cfg.DNSIgnoreStartupFailures = tc.dnsIgnoreStartupFailures

			client, err := NewMemcachedClientWithConfig(
				log.NewNopLogger(),
				t.Name(),
				*cfg,
				prometheus.NewPedanticRegistry(),
			)

			if !tc.dnsIgnoreStartupFailures {
				require.Nil(t, client)
				require.Error(t, err)
				require.Contains(t, err.Error(), "no memcached server addresses were resolved")
				return
			}

			if tc.dnsIgnoreStartupFailures {
				require.NoError(t, err)
				require.NotNil(t, client)

				// Verify that the client is still usable, even if initialization failed
				res := client.GetMulti(context.Background(), []string{"some-key"})
				require.Empty(t, res)
			}
		})
	}
}

func setupDefaultMemcachedClient() (*MemcachedClient, *mockMemcachedClientBackend, error) {
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

func (m *mockMemcachedClientBackend) Add(item *memcache.Item) error {
	if _, ok := m.values[item.Key]; ok {
		return memcache.ErrNotStored
	}

	m.values[item.Key] = item
	return nil
}

func (m *mockMemcachedClientBackend) Delete(key string) error {
	delete(m.values, key)
	return nil
}

func (m *mockMemcachedClientBackend) Touch(key string, seconds int32) error {
	m.values[key].Expiration = seconds
	return nil
}

func (m *mockMemcachedClientBackend) Decrement(key string, delta uint64) (uint64, error) {
	value, _ := strconv.ParseUint(string(m.values[key].Value), 10, 64)
	value -= delta
	m.values[key].Value = []byte(strconv.FormatUint(value, 10))
	return value, nil
}

func (m *mockMemcachedClientBackend) Increment(key string, delta uint64) (uint64, error) {
	value, _ := strconv.ParseUint(string(m.values[key].Value), 10, 64)
	value += delta
	m.values[key].Value = []byte(strconv.FormatUint(value, 10))
	return value, nil
}

func (m *mockMemcachedClientBackend) CompareAndSwap(item *memcache.Item) error {
	m.values[item.Key] = item
	return nil
}

func (m *mockMemcachedClientBackend) FlushAll() error {
	m.values = make(map[string]*memcache.Item)
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
