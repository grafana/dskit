package ring

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func BenchmarkRingRefresh(b *testing.B) {
	for _, instances := range []int{0, 1, 32, 512, 2048} {
		for _, zoned := range []bool{false, true} {
			b.Run(fmt.Sprintf("heartbeat/instances=%d/zoned=%t", instances, zoned), func(b *testing.B) {
				benchmarkRingRefresh(b, instances, zoned, "heartbeat")
			})
		}
	}
	for _, update := range []string{"unchanged", "topology"} {
		b.Run(update+"/instances=512/zoned=true", func(b *testing.B) {
			benchmarkRingRefresh(b, 512, true, update)
		})
	}
}

func benchmarkRingRefresh(b *testing.B, instances int, zoned bool, update string) {
	cfg := Config{HeartbeatTimeout: time.Hour, ReplicationFactor: 3, ZoneAwarenessEnabled: zoned}
	r, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, nil, NewDefaultReplicationStrategy(), prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(b, err)
	first, second := NewDesc(), NewDesc()
	now := time.Now()
	for i := 0; i < instances; i++ {
		tokens := make([]uint32, 128)
		for j := range tokens {
			tokens[j] = uint32(uint64(i*128+j) * (1 << 32) / uint64(instances*128))
		}
		timestamp := now
		if i%7 == 6 {
			timestamp = now.Add(-2 * time.Hour)
		}
		id := fmt.Sprintf("instance-%d", i)
		first.AddIngester(id, id, fmt.Sprintf("zone-%d", i%3), tokens, InstanceState(i%4), now, false, time.Time{}, nil)
		copy := first.Ingesters[id]
		copy.Timestamp = timestamp.Unix()
		first.Ingesters[id] = copy
		if update != "unchanged" {
			copy.Timestamp++
		}
		if update == "topology" && i == 0 {
			copy.Tokens = append([]uint32(nil), tokens...)
			copy.Tokens[0]++
		}
		second.Ingesters[id] = copy
	}
	r.updateRingState(first)
	descriptions := [2]*Desc{second, first}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.updateRingState(descriptions[i%2])
	}
}
