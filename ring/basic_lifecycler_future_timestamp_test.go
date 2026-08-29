package ring

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/services"
)

type noopDNSProvider struct{}

func (noopDNSProvider) Resolve(context.Context, []string) error { return nil }
func (noopDNSProvider) Addresses() []string                     { return nil }

// TestBasicLifecycler_RegisterRecoversFromFutureHeartbeat exercises the full registration flow
// against a real memberlist KV (the backend where the bug reproduces; the in-memory consul mock
// does not run the ring merge). It seeds the instance's own ring entry with a heartbeat timestamp
// far in the future, then starts the lifecycler and asserts that:
//   - registration succeeds (before the fix the memberlist merge returned "no change detected" on
//     every retry, so the lifecycler failed to start and the process CrashLoopBackOff-ed), and
//   - the entry stored in the KV is healed back to ~now (re-read from the store, not from the
//     lifecycler's local copy).
//
// See grafana/loki#21733.
func TestBasicLifecycler_RegisterRecoversFromFutureHeartbeat(t *testing.T) {
	ctx := context.Background()

	var mlCfg memberlist.KVConfig
	flagext.DefaultValues(&mlCfg)
	mlCfg.TCPTransport = memberlist.TCPTransportConfig{BindAddrs: []string{"127.0.0.1"}, BindPort: 0}
	mlCfg.Codecs = []codec.Codec{GetCodec()}

	mkv := memberlist.NewKV(mlCfg, log.NewNopLogger(), noopDNSProvider{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(ctx, mkv))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), mkv) })

	store, err := memberlist.NewClient(mkv, GetCodec())
	require.NoError(t, err)

	// Seed the instance's own ring entry with a corrupted heartbeat timestamp ~100 years ahead.
	futureTS := time.Now().Add(100 * 365 * 24 * time.Hour).Unix()
	require.NoError(t, store.CAS(ctx, testRingKey, func(in interface{}) (interface{}, bool, error) {
		desc := GetOrCreateRingDesc(in)
		desc.Ingesters[testInstanceID] = InstanceDesc{
			Addr:                "127.0.0.1:12345",
			Zone:                zone(1),
			State:               ACTIVE,
			Tokens:              []uint32{1, 2, 3, 4, 5},
			Timestamp:           futureTS,
			RegisteredTimestamp: time.Now().Add(-time.Hour).Unix(),
		}
		return desc, true, nil
	}))

	// Sanity check: the corrupted entry is actually stored.
	require.Equal(t, futureTS, ringDescFromStore(t, store).Ingesters[testInstanceID].Timestamp)

	cfg := prepareBasicLifecyclerConfig()
	delegate := &mockDelegate{
		onRegister: func(_ *BasicLifecycler, _ Desc, _ bool, _ string, _ InstanceDesc) (InstanceState, Tokens) {
			return ACTIVE, Tokens{1, 2, 3, 4, 5}
		},
	}
	lifecycler, err := NewBasicLifecycler(cfg, testRingName, testRingKey, store, delegate, log.NewNopLogger(), nil)
	require.NoError(t, err)

	// Before the fix this fails: registration never makes forward progress and returns
	// "no change detected", so the service does not reach Running.
	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), lifecycler) })

	// The stored entry must be healed to ~now, not left at the corrupted future timestamp.
	healed := ringDescFromStore(t, store).Ingesters[testInstanceID].Timestamp
	assert.Less(t, healed, futureTS, "heartbeat timestamp was not healed; still in the future")
	assert.InDelta(t, time.Now().Unix(), healed, 60, "healed timestamp should be around now")
}

func ringDescFromStore(t *testing.T, store interface {
	Get(context.Context, string) (interface{}, error)
}) *Desc {
	t.Helper()
	val, err := store.Get(context.Background(), testRingKey)
	require.NoError(t, err)
	require.NotNil(t, val)
	return val.(*Desc)
}
