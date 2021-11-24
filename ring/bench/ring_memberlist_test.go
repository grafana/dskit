package bench

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
)

type dnsProviderMock struct {
	resolved []string
}

func (p *dnsProviderMock) Resolve(ctx context.Context, addrs []string) error {
	p.resolved = addrs
	return nil
}

func (p dnsProviderMock) Addresses() []string {
	return p.resolved
}

func encodeMessage(b *testing.B, key string, d *ring.Desc) []byte {
	c := ring.GetCodec()
	val, err := c.Encode(d)
	require.NoError(b, err)

	kvPair := memberlist.KeyValuePair{
		Key:   key,
		Value: val,
		Codec: c.CodecID(),
	}

	ser, err := kvPair.Marshal()
	require.NoError(b, err)
	return ser
}

// Benchmark the memberlist receive path when it is being used as the ring backing store.
func BenchmarkMemberlistReceiveWithRingDesc(b *testing.B) {
	c := ring.GetCodec()

	var cfg memberlist.KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = memberlist.TCPTransportConfig{
		BindAddrs: []string{"localhost"},
	}
	cfg.Codecs = []codec.Codec{c}

	mkv := memberlist.NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), mkv))
	defer services.StopAndAwaitTerminated(context.Background(), mkv) //nolint:errcheck

	// Build the initial ring state:
	// - The ring isn't actually in use, so the fields such as address/zone are not important.
	// - The number of keys in the store has no impact for this test, so simulate a single ring.
	// - The number of instances in the ring does have a big impact.
	const numInstances = 600
	const numTokens = 128
	{
		var tokensUsed []uint32

		initialDesc := ring.NewDesc()
		for i := 0; i < numInstances; i++ {
			tokens := ring.GenerateTokens(numTokens, tokensUsed)
			initialDesc.AddIngester(fmt.Sprintf("instance-%d", i), "127.0.0.1", "zone", tokens, ring.ACTIVE, time.Now())
		}
		// Send a single update to populate the store.
		msg := encodeMessage(b, "ring", initialDesc)
		mkv.NotifyMsg(msg)
	}

	// Pre-encode some payloads. It's not significant what the payloads actually
	// update in the ring, though it may be important for future optimisations.
	testMsgs := make([][]byte, 100)
	for i := range testMsgs {
		testDesc := ring.NewDesc()
		testDesc.AddIngester(fmt.Sprintf("instance-%d", i), "127.0.0.1", "zone", nil, ring.ACTIVE, time.Now())
		testMsgs[i] = encodeMessage(b, "ring", testDesc)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mkv.NotifyMsg(testMsgs[i%len(testMsgs)])
	}
}
