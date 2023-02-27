package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	// var kvcfg memberlist.KVConfig
	// flagext.DefaultValues(&kvcfg)
	// kvcfg.Codecs = []codec.Codec{codec.NewProtoCodec("ringDesc", ring.ProtoDescFactory)}
	// kvcfg.JoinMembers = []string{"127.0.0.1"}

	// kvv := memberlist.NewKV(kvcfg, log.NewNopLogger(), dns.NewProvider(log.NewNopLogger(), nil, "golang"), prometheus.NewPedanticRegistry())

	// kv1, err := memberlist.NewClient(kvv, codec.String{})
	// if err != nil {
	// 	panic(err)
	// }

	// var c ring.ReadRing

	ctx := context.Background()

	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	kvconfig := memberlist.KVConfig{
		Codecs: []codec.Codec{ring.GetCodec()},
		TCPTransport: memberlist.TCPTransportConfig{
			BindPort:  0, // randomize bindport
			BindAddrs: []string{"127.0.0.1"},
		},
		JoinMembers:   []string{"127.0.0.1"},
		NodeName:      "client",
		StreamTimeout: 5 * time.Second, // make it configurable?
		// ClusterLabelVerificationDisabled: true,
	}

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, level.AllowDebug())

	dnsProvider := dns.NewProvider(log.With(logger, "component", "dns"), nil, dns.GolangResolverType)
	MemberlistKV := memberlist.NewKVInitService(&kvconfig, log.With(logger, "component", "memberlist"), dnsProvider, nil)

	kv := kv.Config{Store: "memberlist"}
	kv.StoreConfig.MemberlistKV = MemberlistKV.GetMemberlistKV
	// kv.RegisterFlagsWithPrefix("cluster", "collectors/", flag.NewFlagSet("", flag.PanicOnError))

	rc.KVStore = kv

	c, err := ring.New(rc, "cluster", "collectors/ring", logger, prometheus.NewPedanticRegistry())
	if err != nil {
		panic(err)
	}
	if err := services.StartAndAwaitRunning(ctx, c); err != nil {
		panic(err)
	}
	defer services.StopAndAwaitTerminated(ctx, c)

	var rr ring.ReadRing
	rr = c

	for {

		replicas, err := rr.GetAllHealthy(ring.Read)
		if err != nil {
			fmt.Println("error when getting healthy instances", err)
		}

		for _, v := range replicas.Instances {
			fmt.Println(v.Addr)
		}

		time.Sleep(1 * time.Second)
	}
}
