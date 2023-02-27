package main

import (
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// singleton logger we use everywhere
	logger = log.With(log.NewLogfmtLogger(os.Stdout), level.AllowDebug())
)

// SimpleMemberlistKV returns a memberlist KV as a service. Starting and Stopping the service is upto the caller.
// Caller can create an instance `kv.Client` from returned service by explicity calling `.GetMemberlistKV()`
// which can be used as dependency to create a ring or ring lifecycler.
func SimpleMemberlistKV(bindaddr string, bindport int, joinmembers []string) *memberlist.KVInitService {
	var config memberlist.KVConfig
	flagext.DefaultValues(&config)

	// Codecs is used to tell memberlist library how to serialize/de-serialize the messages between peers.
	// `ring.GetCode()` uses default, which is protobuf.
	config.Codecs = []codec.Codec{ring.GetCodec()}

	// TCPTransport defines what addr and port this particular peer should listen for.
	config.TCPTransport = memberlist.TCPTransportConfig{
		BindPort:  bindport,
		BindAddrs: []string{bindaddr},
	}

	// joinmembers is the address of peer who is already in the memberlist group.
	// Usually be provided if this peer is trying to join existing cluster.
	// Generally you start very first peer without `joinmembers`, but start every
	// other peers with at least one `joinmembers`.
	if len(joinmembers) > 0 {
		config.JoinMembers = joinmembers
	}

	// resolver defines how each peers IP address should be resolved.
	// We use default resolver comes with Go.
	resolver := dns.NewProvider(log.With(logger, "component", "dns"), prometheus.NewPedanticRegistry(), dns.GolangResolverType)

	return memberlist.NewKVInitService(
		&config,
		log.With(logger, "component", "memberlist"),
		resolver,
		prometheus.NewPedanticRegistry(),
	)

}

// SimpleRing returns an instance of `ring.Ring` as a service. Starting and Stopping the service is upto the caller.
func SimpleRing(store kv.Client) (*ring.Ring, error) {
	var config ring.Config
	flagext.DefaultValues(&config)

	return ring.NewWithStoreClientAndStrategy(
		config,
		"local",           // ring name
		"collectors/ring", // prefix key where peers are stored
		store,
		ring.NewDefaultReplicationStrategy(),
		prometheus.NewPedanticRegistry(),
		log.With(logger, "component", "ring"),
	)
}

// SimpleRingLifeCycler returns an instance lifecycler for the given `kv.Client`.
// Usually lifecycler will be part of the server side that act as a single peer.
func SimpleRingLifeCycler(store kv.Client) (*ring.BasicLifecycler, error) {
	var config ring.BasicLifecyclerConfig
	flagext.DefaultValues(&config)

	return ring.NewBasicLifecycler(
		config,
		"local",
		"collectors/ring",
		store,
		nil,
		log.With(logger, "component", "lifecycler"),
		prometheus.NewPedanticRegistry(),
	)
}
