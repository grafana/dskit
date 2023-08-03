package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
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

var (
	mode string

	bindaddr   string
	bindport   int
	joinmember string

	// singleton logger we use everywhere
	logger = log.With(log.NewLogfmtLogger(os.Stdout), level.AllowDebug())
)

func main() {
	flag.StringVar(&bindaddr, "bindaddr", "127.0.0.1", "bindaddr for this specific peer")
	flag.IntVar(&bindport, "bindport", 7946, "bindport for this specific peer")
	flag.StringVar(&joinmember, "join-member", "", "peer addr that is part of existing cluster")
	flag.StringVar(&mode, "mode", "cluster", "cluster or client")

	flag.Parse()

	switch mode {
	case "client":
		runClient()
	case "cluster":
		runCluster()
	default:
		fmt.Println("running in invalid-mode")
		flag.Usage()
	}
}

func runCluster() {
	ctx := context.Background()

	joinmembers := make([]string, 0)
	if joinmember != "" {
		joinmembers = append(joinmembers, joinmember)
	}

	// start memberlist service.
	memberlistsvc := SimpleMemberlistKV(bindaddr, bindport, joinmembers)
	if err := services.StartAndAwaitRunning(ctx, memberlistsvc); err != nil {
		panic(err)
	}
	defer services.StopAndAwaitTerminated(ctx, memberlistsvc)

	store, err := memberlistsvc.GetMemberlistKV()
	if err != nil {
		panic(err)
	}

	client, err := memberlist.NewClient(store, ring.GetCodec())
	if err != nil {
		panic(err)
	}

	lfc, err := SimpleRingLifecycler(client, bindaddr, bindport)
	if err != nil {
		panic(err)
	}

	// start lifecycler service
	if err := services.StartAndAwaitRunning(ctx, lfc); err != nil {
		panic(err)
	}
	defer services.StopAndAwaitTerminated(ctx, lfc)

	listener, err := net.Listen("tcp", bindaddr+":8100")
	if err != nil {
		panic(err)
	}

	fmt.Println("listening on ", listener.Addr())

	mux := http.NewServeMux()
	mux.Handle("/ring", lfc)
	mux.Handle("/kv", memberlistsvc)

	panic(http.Serve(listener, mux))
}

func runClient() {
	ctx := context.Background()

	// start memberlist service.
	memberlistsvc := SimpleMemberlistKV("127.0.0.3", 0, []string{"127.0.0.1"})
	if err := services.StartAndAwaitRunning(ctx, memberlistsvc); err != nil {
		panic(err)
	}
	defer services.StopAndAwaitTerminated(ctx, memberlistsvc)

	store, err := memberlistsvc.GetMemberlistKV()
	if err != nil {
		panic(err)
	}

	client, err := memberlist.NewClient(store, ring.GetCodec())
	if err != nil {
		panic(err)
	}

	ringsvc, err := SimpleRing(client)
	if err != nil {
		panic(err)
	}

	// start the ring service
	if err := services.StartAndAwaitRunning(ctx, ringsvc); err != nil {
		panic(err)
	}
	defer services.StopAndAwaitTerminated(ctx, ringsvc)

	for {
		time.Sleep(1 * time.Second)

		replicas, err := ringsvc.GetAllHealthy(ring.Read)
		if err != nil {
			fmt.Println("error when getting healthy instances", err)
			continue
		}

		fmt.Println("Peers:")
		for _, v := range replicas.Instances {
			fmt.Println("Addr", v.Addr, "Token count", len(v.Tokens))
		}
	}

}

// SimpleMemberlistKV returns a memberlist KV as a service. Starting and stopping the service is up to the caller.
// Caller can create an instance `kv.Client` from returned service by explicity calling `.GetMemberlistKV()`
// which can be used as dependency to create a ring or ring lifecycler.
func SimpleMemberlistKV(bindaddr string, bindport int, joinmembers []string) *memberlist.KVInitService {
	var config memberlist.KVConfig
	flagext.DefaultValues(&config)

	// Codecs is used to tell memberlist library how to serialize/de-serialize the messages between peers.
	// `ring.GetCodec()` uses default, which is protobuf.
	config.Codecs = []codec.Codec{ring.GetCodec()}

	// TCPTransport defines what addr and port this particular peer should listen on.
	config.TCPTransport = memberlist.TCPTransportConfig{
		BindPort:  bindport,
		BindAddrs: []string{bindaddr},
	}

	// joinmembers are the addresses of peers who are already in the memberlist group.
	// Usually provided if this peer is trying to join an existing cluster.
	// Generally you start the very first peer without `joinmembers`, but start all
	// other peers with at least one `joinmembers`.
	if len(joinmembers) > 0 {
		config.JoinMembers = joinmembers
	}

	// resolver defines how each peers IP address should be resolved.
	// We use default resolver comes with Go.
	resolver := dns.NewProvider(log.With(logger, "component", "dns"), prometheus.NewPedanticRegistry(), dns.GolangResolverType)

	config.NodeName = bindaddr
	config.StreamTimeout = 5 * time.Second

	return memberlist.NewKVInitService(
		&config,
		log.With(logger, "component", "memberlist"),
		resolver,
		prometheus.NewPedanticRegistry(),
	)

}

// SimpleRing returns an instance of `ring.Ring` as a service. Starting and stopping the service is up to the caller.
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

// SimpleRingLifecycler returns an instance lifecycler for the given `kv.Client`.
// Usually lifecycler will be part of the server side that act as a single peer.
func SimpleRingLifecycler(store kv.Client, bindaddr string, bindport int) (*ring.BasicLifecycler, error) {
	var config ring.BasicLifecyclerConfig
	config.ID = bindaddr
	config.Addr = fmt.Sprintf("%s:%d", bindaddr, bindport)

	var delegate ring.BasicLifecyclerDelegate

	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, 128)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = ring.NewAutoForgetDelegate(1*time.Minute, delegate, logger)

	return ring.NewBasicLifecycler(
		config,
		"local",
		"collectors/ring",
		store,
		delegate,
		log.With(logger, "component", "lifecycler"),
		prometheus.NewPedanticRegistry(),
	)
}
