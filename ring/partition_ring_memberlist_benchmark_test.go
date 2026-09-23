package ring

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/services"
)

func BenchmarkPartitionRingMemberlist(b *testing.B) {
	for _, partitions := range []int{0, 1, 32, 512} {
		for _, refresh := range []bool{false, true} {
			b.Run(fmt.Sprintf("partitions=%d/refresh=%t", partitions, refresh), func(b *testing.B) {
				ctx := context.Background()
				logger := log.NewNopLogger()
				c := GetPartitionRingCodec()
				var cfg memberlist.KVConfig
				flagext.DefaultValues(&cfg)
				cfg.TCPTransport = memberlist.TCPTransportConfig{BindAddrs: []string{"127.0.0.1"}}
				cfg.Codecs = []codec.Codec{c}
				store := memberlist.NewKV(cfg, logger, nil, prometheus.NewRegistry())
				require.NoError(b, services.StartAndAwaitRunning(ctx, store))
				b.Cleanup(func() { require.NoError(b, services.StopAndAwaitTerminated(ctx, store)) })
				client, err := memberlist.NewClient(store, c)
				require.NoError(b, err)
				desc := NewPartitionRingDesc()
				for p := 0; p < partitions; p++ {
					tokens := make([]uint32, optimalTokensPerInstance)
					for i := range tokens {
						tokens[i] = uint32(uint64(i*partitions+p) * (1 << 32) / uint64(partitions*len(tokens)))
					}
					desc.Partitions[int32(p)] = PartitionDesc{Id: int32(p), Tokens: tokens, State: PartitionActive, StateTimestamp: 1}
					for zone := 0; zone < 3; zone++ {
						desc.Owners[fmt.Sprintf("zone-%d-%d", zone, p)] = OwnerDesc{OwnedPartition: int32(p), State: OwnerActive, UpdatedTimestamp: 1}
					}
				}
				if partitions > 0 {
					require.NoError(b, client.CAS(ctx, "ring", func(interface{}) (interface{}, bool, error) { return desc, false, nil }))
				}
				watcher := NewPartitionRingWatcher("benchmark", "ring", client, logger, prometheus.NewRegistry())
				require.NoError(b, watcher.starting(ctx))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if refresh {
						if err := watcher.starting(ctx); err != nil {
							b.Fatal(err)
						}
					} else {
						value, err := client.Get(ctx, "ring")
						if err != nil {
							b.Fatal(err)
						}
						if (partitions == 0 && value != nil) || (partitions > 0 && len(value.(*PartitionRingDesc).Partitions) != partitions) {
							b.Fatal("unexpected partition count")
						}
					}
				}
			})
		}
	}
}
