package kv

import (
	"context"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.uber.org/atomic"
)

// The mockClient does not anything.
// This is used for testing only.
type mockClient struct{}

func buildMockClient(logger log.Logger) (Client, error) {
	level.Warn(logger).Log("msg", "created mockClient for testing only")
	return mockClient{}, nil
}

func (m mockClient) List(_ context.Context, _ string) ([]string, error) {
	return []string{}, nil
}

func (m mockClient) Get(_ context.Context, _ string) (interface{}, error) {
	return "", nil
}

func (m mockClient) Delete(_ context.Context, _ string) error {
	return nil
}

func (m mockClient) CAS(_ context.Context, _ string, _ func(in interface{}) (out interface{}, retry bool, err error)) error {
	return nil
}

func (m mockClient) WatchKey(_ context.Context, _ string, _ func(interface{}) bool) {
}

func (m mockClient) WatchPrefix(_ context.Context, _ string, _ func(string, interface{}) bool) {
}

// MockCountingClient is a wrapper around the Client interface that counts the number of times its functions are called.
// This is used for testing only.
type MockCountingClient struct {
	client Client

	mtx   sync.Mutex
	calls map[string]*atomic.Int32
}

func NewMockCountingClient(client Client) *MockCountingClient {
	return &MockCountingClient{
		client: client,
		calls:  make(map[string]*atomic.Int32),
	}
}

func (mc *MockCountingClient) List(ctx context.Context, prefix string) ([]string, error) {
	mc.incCall("List")

	return mc.client.List(ctx, prefix)
}
func (mc *MockCountingClient) Get(ctx context.Context, key string) (interface{}, error) {
	mc.incCall("Get")

	return mc.client.Get(ctx, key)
}

func (mc *MockCountingClient) Delete(ctx context.Context, key string) error {
	mc.incCall("Delete")

	return mc.client.Delete(ctx, key)
}

func (mc *MockCountingClient) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	mc.incCall("CAS")

	return mc.client.CAS(ctx, key, f)
}

func (mc *MockCountingClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	mc.incCall("WatchKey")

	mc.client.WatchKey(ctx, key, f)
}

func (mc *MockCountingClient) WatchPrefix(ctx context.Context, key string, f func(string, interface{}) bool) {
	mc.incCall("WatchPrefix")

	mc.client.WatchPrefix(ctx, key, f)
}

func (mc *MockCountingClient) GetCalls(name string) int {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()
	count, exists := mc.calls[name]

	if !exists {
		return 0
	}

	return int(count.Load())
}

func (mc *MockCountingClient) incCall(name string) {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	if _, exists := mc.calls[name]; !exists {
		mc.calls[name] = atomic.NewInt32(0)
	}

	value := mc.calls[name]
	value.Add(1)
}
